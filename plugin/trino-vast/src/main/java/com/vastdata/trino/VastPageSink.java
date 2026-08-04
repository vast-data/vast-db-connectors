/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.buffering.BufferedDml;
import com.vastdata.client.buffering.BufferedTaskFactory;
import com.vastdata.client.buffering.VsrAppender;
import com.vastdata.client.buffering.insert.BufferedInsertTaskFactory;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.client.executor.RetryStrategy;
import com.vastdata.client.executor.RetryStrategyFactory;
import com.vastdata.client.importdata.ImportDataExecutor;
import com.vastdata.client.metrics.BufferedInsertMetrics;
import com.vastdata.client.metrics.ByColumnInserterMetrics;
import com.vastdata.client.metrics.InsertedRowsStats;
import com.vastdata.client.metrics.RecordBatchSplitterMetrics;
import com.vastdata.client.metrics.TimeMeasure;
import com.vastdata.client.schema.ImportDataContext;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.trino.metrics.PageSinkMetrics;
import com.vastdata.trino.partition.PartitionKeyHashFunction;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.vastdata.client.error.VastExceptionFactory.hasInterruptException;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.trino.VastSessionProperties.getImportChunkLimit;
import static com.vastdata.trino.VastSessionProperties.getParallelImport;
import static com.vastdata.trino.VastSessionProperties.getRetryMaxCount;
import static com.vastdata.trino.VastSessionProperties.getRetrySleepDuration;

public class VastPageSink
        implements ConnectorPageSink
{
    private static final Logger LOG = Logger.get(VastPageSink.class);

    private final VastClient client;
    private final VastTransactionHandle transaction;
    private final VastInsertTableHandle handle;
    private final Schema schema;
    private final ConnectorSession session;
    private final VastTrinoExceptionFactory vastTrinoExceptionFactory = new VastTrinoExceptionFactory();
    private final VastTraceToken traceToken;
    private final int importChunkLimit;
    private final List<URI> dataEndpoints;
    private final BiFunction<Page, Integer, Long> rowBufferAssigner;
    private final BufferedDml bufferedInserter;
    private final BufferAllocator allocator;
    private final PageSinkMetrics metrics;

    private final TimeMeasure appendPageTimeMeasure;
    private final TimeMeasure finishTimeMeasure;

    private final InsertedRowsStats insertedRowsStats;
    private long rowsReceivedByPageSink;

    private final PageBuffer pageBuffer;
    private final List<Type> types;

    public VastPageSink(VastClient client,
                        VastConfig config,
                        ConnectorSession session,
                        VastTransactionHandle transaction,
                        VastInsertTableHandle handle,
                        List<URI> shuffledDataEndpoints,
                        ConnectorPageSinkId pageSinkId,
                        BufferAllocator insertBuffersAllocator,
                        TypeOperators typeOperators,
                        RecordBatchSplitterMetrics globalRecordBatchSplitterMetrics,
                        BufferedInsertMetrics globalBufferedInsertMetrics,
                        ByColumnInserterMetrics globalByColumnInserterMetrics,
                        PageSinkMetrics metrics,
                        VastIoExecutor vastIoExecutor,
                        VastCpuExecutor vastCpuExecutor)
    {
        LOG.info("page sink created, using endpoints: %s",
                shuffledDataEndpoints);
        this.traceToken = transaction.generateTraceToken(
                session.getTraceToken());
        this.client = client;

        this.transaction = transaction;
        this.handle = handle;
        this.allocator = insertBuffersAllocator.newChildAllocator(
                String.format("PageSink-%d", pageSinkId.getId()), 0,
                Long.MAX_VALUE);

        this.insertedRowsStats = new InsertedRowsStats();
        this.rowsReceivedByPageSink = 0;

        VastTableHandle table = handle.getTable();

        BufferedTaskFactory factory = new BufferedInsertTaskFactory(client,
                config, globalRecordBatchSplitterMetrics,
                globalByColumnInserterMetrics, table.getRowIdStrategyType(),
                table.getSchemaName(), table.getTableName(), transaction,
                shuffledDataEndpoints,
                table.getExtraQueryParams(), session.getUser(),
                table.getIsNonUpdateableColumnPredicate(), insertedRowsStats,
                vastIoExecutor.getExecutor(), vastCpuExecutor.getExecutor(),
                traceToken.toString());

        BufferedDml.Config bufferedInserterConfig = new BufferedDml.Config(
                VastSessionProperties.getInsertBuffersOpenBufferRowCount(
                        session),
                VastSessionProperties.getMaxRequestBodySize(session),
                VastSessionProperties.getInsertBufferOpenVsrCountPreallocation(
                        session),
                VastSessionProperties.getInsertBuffersTargetNodeMaxBufferSize(
                        session),
                VastSessionProperties.getInsertBufferTargetRowCountPerPartitionFlush(
                        session), config.getBufferedInserterMaxWritePermits(),
                config.getBufferedInserterMaxJobPermits());
        this.bufferedInserter = new BufferedDml(bufferedInserterConfig,
                allocator, insertBuffersAllocator, globalBufferedInsertMetrics,
                factory, insertedRowsStats, vastIoExecutor.getExecutor(),
                vastCpuExecutor.getExecutor());

        this.metrics = metrics;

        final List<VastColumnHandle> columns = handle.getColumns();
        final List<Field> fields = columns
                .stream()
                .map(VastColumnHandle::getField)
                .toList();
        this.schema = new Schema(fields);

        this.session = session;
        this.importChunkLimit = getImportChunkLimit(session);
        this.dataEndpoints = shuffledDataEndpoints;
        this.rowBufferAssigner = handle
                .getPartitioning()
                .map(p -> (BiFunction<Page, Integer, Long>) PartitionKeyHashFunction.create(
                        p.partitionFunctions(), typeOperators,
                        PartitionKeyHashFunction.IndexBase.BY_COLUMN_INDEX))
                .orElse((_, _) -> (long) 0);

        this.appendPageTimeMeasure = new TimeMeasure();
        this.finishTimeMeasure = new TimeMeasure();

        this.metrics.incPageSinkCreated();

        this.pageBuffer = new PageBuffer();

        this.types = schema.getFields().stream().map(TypeUtils::convertArrowFieldToTrinoType).collect(Collectors.toList());
    }

    @Override
    public long getCompletedBytes()
    {
        return ConnectorPageSink.super.getCompletedBytes();
    }

    @Override
    public long getMemoryUsage()
    {
        return 0L;
    }

    @Override
    public long getValidationCpuNanos()
    {
        return ConnectorPageSink.super.getValidationCpuNanos();
    }

    private CompletableFuture<?> appendPageForImportData(Page page)
    {
        try (BufferAllocator allocator = this.allocator.newChildAllocator(
                "import-data", 0, Long.MAX_VALUE)) {
            final String endUser = session.getUser();

            VastTableHandle table = handle.getTable();
            LOG.debug(
                    "ImportData(%s): Adapting page: number_of_columns=%s, number_of_rows=%s",
                    traceToken, page.getChannelCount(),
                    page.getPositionCount());
            try (ImportDataContext ctx = new VastTrinoSchemaAdaptor()
                    .adaptForImportData(table, page, this.schema, allocator)
                    .withChunkLimit(importChunkLimit)) {
                final int retryMaxCount = getRetryMaxCount(session);
                final int retrySleepDuration = getRetrySleepDuration(session);
                boolean parallelImport = getParallelImport(session);
                Supplier<RetryStrategy> retryStrategy = () -> RetryStrategyFactory.fixedSleepBetweenRetries(
                        retryMaxCount, retrySleepDuration);

                ImportDataExecutor<VastTransactionHandle> executor = new ImportDataExecutor<>(
                        client);

                try {
                    executor.execute(ctx, transaction, traceToken,
                            dataEndpoints, retryStrategy, parallelImport,
                            table.getExtraQueryParams(), endUser);
                }
                catch (VastException e) {
                    throw vastTrinoExceptionFactory.fromVastException(e);
                }
            }
        }

        return NOT_BLOCKED;
    }

    private CompletableFuture<?> appendPageRegular(Page page)
            throws VastRuntimeException
    {
        pageBuffer.addPage(page);
        if (pageBuffer.getRowCount() >= VastSessionProperties.getMaxPageBufferRowCount(session)) {
            return flushPages(pageBuffer.takePages());
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<?> flushPages(List<Page> pages)
            throws VastRuntimeException
    {
        if (pages.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        Map<Long, VsrAppender> bufferIdToAppender = PageToVsr.getPageToBufferVsrAppender(
                pages, rowBufferAssigner, allocator, schema, types, metrics::recordVsrBuildTime);

        try {
            return bufferedInserter.write(bufferIdToAppender);
        }
        catch (VastRuntimeException e) {
            for (VsrAppender vsrAppender : bufferIdToAppender.values()) {
                try {
                    vsrAppender.close();
                }
                catch (Throwable closeEx) {
                    e.addSuppressed(closeEx);
                }
            }
            throw e;
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        rowsReceivedByPageSink += page.getPositionCount();

        int pageId = page.hashCode();
        LOG.debug("appendPage got page with %d positions. pageId: %d",
                page.getPositionCount(), pageId);

        appendPageTimeMeasure.start(metrics::recordAppendPageIdleTime);

        metrics.recordIncomingPage(page.getPositionCount());
        CompletableFuture<?> future;
        try {
            if (handle.isForImportData()) {
                future = appendPageForImportData(page);
            }
            else {
                future = appendPageRegular(page);
            }
        }
        catch (VastRuntimeException re) {
            // TODO need to fix error handling so BufferedDml will not obscure user exceptions
            //      and that TrinoExceptions will be created only in the spi scope
            if (re.getCause() instanceof TrinoException trinoException) {
                throw trinoException;
            }
            throw vastTrinoExceptionFactory.fromVastRuntimeException(re);
        }
        catch (Throwable any) {
            if (hasInterruptException(any)) {
                Thread.currentThread().interrupt();
            }
            throw toRuntime(any);
        }

        return future.whenComplete((_, _) ->
        {
            appendPageTimeMeasure.end(metrics::recordAppendPageExecTime);
        });
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        LOG.info("finish");
        finishTimeMeasure.start(metrics::recordFinishIdleTime);
        try {
            flushPages(pageBuffer.takePages()).join();
            bufferedInserter.flushAllAndFinish();
        }
        catch (VastRuntimeException e) {
            // TODO need to fix error handling so BufferedDml will not obscure user exceptions
            //      and that TrinoExceptions will be created only in the spi scope
            if (e.getCause() instanceof TrinoException trinoException) {
                throw trinoException;
            }
            throw vastTrinoExceptionFactory.fromVastRuntimeException(e);
        }

        close();

        finishTimeMeasure.end(metrics::recordFinishExecTime);
        return CompletableFuture.completedFuture(List.of());
    }

    @Override
    public void abort()
    {
        LOG.warn("abort");
        bufferedInserter.abort();
        close();
    }

    private void close()
    {
        if (!handle.isForImportData()) {
            LOG.info(
                    "Row Statistics: page sink received %d rows, buffered insert stats: %s, traceToken: %s",
                    rowsReceivedByPageSink, insertedRowsStats, traceToken);
        }
        this.metrics.incPageSinkClosed();

        bufferedInserter.close();
        allocator.close();
    }

    public List<URI> getShuffledDataEndpoints()
    {
        return this.dataEndpoints;
    }

    private static class PageBuffer
    {
        private final List<Page> pageBuffer;
        private int rowCount;

        public PageBuffer()
        {
            this.pageBuffer = new ArrayList<>();
            this.rowCount = 0;
        }

        public List<Page> takePages()
        {
            List<Page> pages = new ArrayList<>(pageBuffer);
            pageBuffer.clear();
            rowCount = 0;
            return pages;
        }

        public void addPage(Page page)
        {
            pageBuffer.add(page);
            rowCount += page.getPositionCount();
        }

        public int getRowCount()
        {
            return rowCount;
        }
    }
}
