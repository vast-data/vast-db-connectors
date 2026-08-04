/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.buffering.insert;

import com.vastdata.client.QueryDataExtraParams;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.buffering.Buffer;
import com.vastdata.client.buffering.BufferedTaskFactory;
import com.vastdata.client.bycolumninserter.ByColumnInserter;
import com.vastdata.client.metrics.BufferedInsertMetrics;
import com.vastdata.client.metrics.ByColumnInserterMetrics;
import com.vastdata.client.metrics.InsertedRowsStats;
import com.vastdata.client.metrics.RecordBatchSplitterMetrics;
import com.vastdata.client.rowid.RowIDStrategyType;
import com.vastdata.client.tx.VastTransaction;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BufferedInsertTaskFactory
        implements BufferedTaskFactory
{
    private final VastClient vastClient;
    private final VastConfig vastConfig;

    private final ExecutorService ioExecutor;
    private final ExecutorService cpuExecutor;

    private final RecordBatchSplitterMetrics splitterMetrics;
    private final ByColumnInserterMetrics metrics;
    private final RowIDStrategyType rowIdType;
    private final String schema;
    private final String table;
    private final VastTransaction transaction;
    private final List<URI> dataEndpoints;
    private final QueryDataExtraParams extraQueryParams;
    private final String endUser;
    private final Predicate<String> isNonUpdateableColumn;
    private final InsertedRowsStats insertedRowsStats;
    private final String traceToken;

    public BufferedInsertTaskFactory(VastClient vastClient,
            VastConfig vastConfig, RecordBatchSplitterMetrics splitterMetrics,
            ByColumnInserterMetrics metrics, RowIDStrategyType rowIdType,
            String schema, String table, VastTransaction transaction,
            List<URI> dataEndpoints, QueryDataExtraParams extraQueryParams, String endUser,
            Predicate<String> isNonUpdateableColumn,
            InsertedRowsStats insertedRowsStats, ExecutorService ioExecutor,
            ExecutorService cpuExecutor, String traceToken)
    {
        this.vastClient = vastClient;
        this.vastConfig = vastConfig;

        this.ioExecutor = ioExecutor;
        this.cpuExecutor = cpuExecutor;

        this.splitterMetrics = splitterMetrics;
        this.metrics = metrics;
        this.rowIdType = rowIdType;
        this.schema = schema;
        this.table = table;
        this.transaction = transaction;
        this.dataEndpoints = dataEndpoints;
        this.extraQueryParams = extraQueryParams;
        this.endUser = endUser;
        this.isNonUpdateableColumn = isNonUpdateableColumn;
        this.insertedRowsStats = insertedRowsStats;
        this.traceToken = traceToken;
    }

    @Override
    public CompletableFuture<Void> executeAsync(List<Buffer> buffers,
            BufferedInsertMetrics bufferedInsertMetrics,
            BufferAllocator allocator)
    {
        ByColumnInserter inserter = new ByColumnInserter(vastClient, vastConfig,
                rowIdType, schema, table, transaction, dataEndpoints, extraQueryParams, endUser,
                splitterMetrics, metrics, isNonUpdateableColumn,
                insertedRowsStats, ioExecutor, cpuExecutor, traceToken);
        BufferAllocator insertTaskAllocator = allocator.newChildAllocator(
                "insertTask", 0, Long.MAX_VALUE);
        List<VectorSchemaRoot> allVsrs = buffers.stream().flatMap(buffer -> {
            Stream<VectorSchemaRoot> vsrs = buffer
                    .moveVsrs(insertTaskAllocator)
                    .stream();
            buffer.close();
            return vsrs;
        }).collect(Collectors.toList());

        BufferAllocator resultRowIdAllocator = allocator.newChildAllocator(
                "result-row-id-allocator", 0, Long.MAX_VALUE);

        return inserter
                .insert(allVsrs, resultRowIdAllocator)
                .whenComplete((resultVsr, err) -> {
                    try {
                        if (resultVsr != null) {
                            resultVsr.close();
                        }
                    }
                    finally {
                        resultRowIdAllocator.close();
                        insertTaskAllocator.close();
                    }
                })
                .thenApply(v -> (Void) null);
    }

    @Override
    public String getTaskName()
    {
        return "Insert";
    }
}
