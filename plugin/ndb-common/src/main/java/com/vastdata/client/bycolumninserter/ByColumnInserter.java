/*
 * Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.bycolumninserter;

import com.vastdata.client.QueryDataExtraParams;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.metrics.ByColumnInserterMetrics;
import com.vastdata.client.metrics.InsertedRowsStats;
import com.vastdata.client.metrics.RecordBatchSplitterMetrics;
import com.vastdata.client.rowid.RowIDStrategyType;
import com.vastdata.client.rowid.RowIdListSchemaFactory;
import com.vastdata.client.tx.VastTransaction;
import io.airlift.log.Logger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static java.lang.String.format;

public class ByColumnInserter
{
    private static final Logger LOG = Logger.get(ByColumnInserter.class);
    private final VastClient vastClient;
    private final VastConfig vastConfig;

    private final ExecutorService ioExecutor;
    private final ExecutorService cpuExecutor;

    private final RowIDStrategyType rowIdType;
    private final String path;
    private final VastTransaction transaction;
    private final List<URI> dataEndpoints;
    private final QueryDataExtraParams extraQueryParams;
    private final String endUser;
    private final RecordBatchSplitterMetrics splitterMetrics;
    private final ByColumnInserterMetrics metrics;
    private final Predicate<String> isNonUpdateableColumn;
    private final InsertedRowsStats insertedRowsStats;
    private final String traceToken;
    private final ByColumnSerializer serializer;

    public ByColumnInserter(VastClient vastClient, VastConfig vastConfig,
                            RowIDStrategyType rowIdType, String schema, String table,
                            VastTransaction transaction, List<URI> dataEndpoints,
                            QueryDataExtraParams extraQueryParams, String endUser, RecordBatchSplitterMetrics splitterMetrics,
            ByColumnInserterMetrics metrics,
            Predicate<String> isNonUpdateableColumn,
            InsertedRowsStats insertedRowsStats, ExecutorService ioExecutor,
            ExecutorService cpuExecutor, String traceToken)
    {
        this.vastClient = vastClient;
        this.vastConfig = vastConfig;
        this.ioExecutor = ioExecutor;
        this.cpuExecutor = cpuExecutor;
        this.rowIdType = rowIdType;
        this.transaction = transaction;
        this.extraQueryParams = extraQueryParams;
        this.endUser = endUser;
        this.dataEndpoints = dataEndpoints;
        this.insertedRowsStats = insertedRowsStats;
        this.path = format("/%s/%s", schema, table);
        this.splitterMetrics = splitterMetrics;
        this.metrics = metrics;
        this.isNonUpdateableColumn = isNonUpdateableColumn;
        this.traceToken = traceToken;
        this.serializer = new ByColumnSerializer(vastConfig, splitterMetrics,
                rowIdType, traceToken);
    }

    public CompletableFuture<VectorSchemaRoot> insert(
            List<VectorSchemaRoot> vsrs, BufferAllocator resultAllocator)
    {
        int totalRowCount = vsrs.stream().mapToInt(VectorSchemaRoot::getRowCount).sum();
        insertedRowsStats.addRowIdsReturnedFromInsert(totalRowCount);

        List<VectorSchemaRoot> nonEmptyVsrs = vsrs
                .stream()
                .filter(v -> v.getRowCount() > 0)
                .collect(Collectors.toList());
        if (nonEmptyVsrs.isEmpty()) {
            return CompletableFuture.completedFuture(VectorSchemaRoot.create(
                    RowIdListSchemaFactory.get(rowIdType), resultAllocator));
        }

        BufferAllocator workAllocator = resultAllocator.newChildAllocator(
                "executeWorkAllocator", 0, Long.MAX_VALUE);
        ExecutionContext context = new ExecutionContext(workAllocator);
        ExecuteFuncs executeFuncs = new ExecuteFuncs(vastClient, vastConfig,
                rowIdType, path, transaction, dataEndpoints, extraQueryParams,
                endUser, metrics, traceToken);

        return CompletableFuture
                .runAsync(() -> planInsert(nonEmptyVsrs, context, serializer,
                        workAllocator), cpuExecutor)
                .thenCompose(ignored -> executeInsert(context, executeFuncs,
                        resultAllocator))
                .thenRunAsync(
                        () -> planUpdate(context, serializer, resultAllocator))
                .thenCompose(ignored -> executeUpdate(context, executeFuncs))
                .thenApply(ignored -> context.getInsertRowIds())
                .whenComplete((res, ex) -> {
                    cleanupContext(context, ex);
                    vsrs.forEach(VectorSchemaRoot::close);
                });
    }

    private void planInsert(List<VectorSchemaRoot> vsrs,
            ExecutionContext context, ByColumnSerializer serializer,
            BufferAllocator workAllocator)
    {
        try {
            ByColumnSerializer.InsertPlan plan = serializer.makeInsertPlan(vsrs,
                    isNonUpdateableColumn, vastConfig, workAllocator);
            context.stepOneFinish(plan.insertPayloads, plan.updateVsrs);
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
    }

    private CompletableFuture<Void> executeInsert(ExecutionContext context,
            ExecuteFuncs executeFuncs, BufferAllocator resultAllocator)
    {
        List<byte[]> payloads = context.takeInsertPayloads();
        List<CompletableFuture<VectorSchemaRoot>> futures = payloads
                .stream()
                .map(payload -> CompletableFuture.supplyAsync(() -> {
                    try {
                        metrics.recordInsertSerialized(payload.length,
                                vastConfig.getMaxRequestBodySize());
                        return executeFuncs.insert(payload,
                                context.getWorkAllocator());
                    }
                    catch (Exception e) {
                        throw toRuntime(e);
                    }
                }, ioExecutor))
                .collect(Collectors.toList());

        return CompletableFuture
                .allOf(futures.toArray(new CompletableFuture[0]))
                .thenAccept(v -> {
                    List<VectorSchemaRoot> rowIds = null;
                    try {
                        rowIds = futures
                                .stream()
                                .map(CompletableFuture::join)
                                .collect(Collectors.toList());
                        VectorSchemaRoot allRowIds = VectorSchemaRoot.create(
                                RowIdListSchemaFactory.get(rowIdType),
                                resultAllocator);
                        insertedRowsStats.addRowIdsReturnedFromInsert(allRowIds.getRowCount());
                        VectorSchemaRootAppender.append(false, allRowIds,
                                rowIds.toArray(new VectorSchemaRoot[0]));
                        context.stepTwoFinish(allRowIds);
                    }
                    finally {
                        if (rowIds != null) {
                            rowIds.forEach(VectorSchemaRoot::close);
                        }
                    }
                });
    }

    private void planUpdate(ExecutionContext context,
            ByColumnSerializer serializer, BufferAllocator resultAllocator)
    {
        try {
            List<byte[]> updatePayloads = new ArrayList<>();
            int offset = 0;
            for (VectorSchemaRoot updateVsr : context.getUpdateVsrs()) {
                VectorSchemaRoot slicedRowIds = context
                        .getInsertRowIds()
                        .slice(offset, updateVsr.getRowCount());
                updatePayloads.addAll(
                        serializer.serializeUpdate(slicedRowIds, updateVsr,
                                resultAllocator));
                offset += updateVsr.getRowCount();
            }
            context.stepThreeFinish(updatePayloads);
        }
        catch (Exception e) {
            throw new RuntimeException("UpdatePlanTask failed", e);
        }
    }

    private CompletableFuture<Void> executeUpdate(ExecutionContext context,
            ExecuteFuncs executeFuncs)
    {
        List<byte[]> updatePayloads = context.takeUpdatePayloads();
        if (updatePayloads == null || updatePayloads.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<?>[] futures = updatePayloads
                .stream()
                .map(payload -> CompletableFuture.runAsync(() -> {
                    try {
                        metrics.recordUpdateSerialized(payload.length,
                                vastConfig.getMaxRequestBodySize());
                        executeFuncs.update(payload);
                    }
                    catch (Exception e) {
                        throw new RuntimeException(
                                "UpdateExecTask: error executing update", e);
                    }
                }, ioExecutor))
                .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(futures);
    }

    private void cleanupContext(ExecutionContext context, Throwable ex)
    {
        if (ex != null) {
            VectorSchemaRoot root = context.getInsertRowIds();
            if (root != null) {
                root.close();
            }
            LOG.info("%s Pipeline failed. %s", traceToken, ex);
        }
        context.close();
    }
}
