/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.client.error.VastTooLargePageException;
import com.vastdata.client.executor.RetryStrategy;
import com.vastdata.client.executor.RetryStrategyFactory;
import com.vastdata.client.importdata.ImportDataExecutor;
import com.vastdata.client.schema.ImportDataContext;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.net.URI;
import java.util.Collection;
import java.util.Optional;
import java.util.List;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.trino.VastSessionProperties.getImportChunkLimit;
import static com.vastdata.trino.VastSessionProperties.getMaxRowsPerInsert;
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
    private final VastRecordBatchBuilder builder;
    private final Schema schema;
    private final ConnectorSession session;
    private final VastTrinoExceptionFactory vastTrinoExceptionFactory = new VastTrinoExceptionFactory();
    private final VastTraceToken traceToken;
    private final int maxRowsPerInsert;
    private final int importChunkLimit;
    private long pageCount;
    private final List<URI> dataEndpoints;

    public VastPageSink(VastClient client, ConnectorSession session, VastTransactionHandle transaction, VastInsertTableHandle handle, List<URI> shuffledDataEndpoints)
    {
        this.traceToken = transaction.generateTraceToken(session.getTraceToken());
        this.client = client;
        this.transaction = transaction;
        this.handle = handle;
        final List<VastColumnHandle> columns = handle.getColumns();
        final List<Field> fields = columns.stream().map(VastColumnHandle::getField).toList();
        this.schema = new Schema(fields);
        this.session = session;
        this.builder = new VastRecordBatchBuilder(schema);
        this.maxRowsPerInsert = getMaxRowsPerInsert(session);
        this.importChunkLimit = getImportChunkLimit(session);
        this.dataEndpoints = shuffledDataEndpoints;
    }

    @Override
    public long getCompletedBytes()
    {
        return ConnectorPageSink.super.getCompletedBytes();
    }

    @Override
    public long getMemoryUsage()
    {
        return ConnectorPageSink.super.getMemoryUsage();
    }

    @Override
    public long getValidationCpuNanos()
    {
        return ConnectorPageSink.super.getValidationCpuNanos();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        try {
            VastTableHandle table = handle.getTable();
            if (handle.isForImportData()) {
                LOG.debug("ImportData(%s): Adapting page: number_of_columns=%s, number_of_rows=%s", traceToken, page.getChannelCount(), page.getPositionCount());
                ImportDataContext ctx = new VastTrinoSchemaAdaptor()
                        .adaptForImportData(table, page, this.schema)
                        .withChunkLimit(importChunkLimit);
                try {
                    final int retryMaxCount = getRetryMaxCount(session);
                    final int retrySleepDuration = getRetrySleepDuration(session);
                    boolean parallelImport = getParallelImport(session);
                    Supplier<RetryStrategy> retryStrategy = () -> RetryStrategyFactory.fixedSleepBetweenRetries(retryMaxCount, retrySleepDuration);
                    new ImportDataExecutor<VastTransactionHandle>(client).execute(ctx, transaction, traceToken, dataEndpoints, retryStrategy, parallelImport);
                }
                catch (VastException e) {
                    throw vastTrinoExceptionFactory.fromVastException(e);
                }
                catch (VastRuntimeException re) {
                    throw vastTrinoExceptionFactory.fromVastRuntimeException(re);
                }
                catch (Throwable any) {
                    throw toRuntime(any);
                }
            }
            else {
                URI endpoint = dataEndpoints.get((int) (pageCount % dataEndpoints.size()));
                try (VectorSchemaRoot root = builder.build(page)) {
                    client.insertRows(transaction, table.getSchemaName(), table.getTableName(), root, endpoint, Optional.of(maxRowsPerInsert));
                }
                catch (VastTooLargePageException e) {
                    throw new TrinoException(StandardErrorCode.PAGE_TOO_LARGE, e);
                }
                catch (VastException e) {
                    throw toRuntime(e);
                }
                pageCount += 1;
            }
        }
        catch (Throwable e) {
            LOG.error(e, "appendPage(%s) failed: %s", traceToken, e);
            throw e;
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return CompletableFuture.completedFuture(List.of());
    }

    @Override
    public void abort()
    {
        LOG.warn("aborting page sink");
    }

    public List<URI> getShuffledDataEndpoints()
    {
        return this.dataEndpoints;
    }
}
