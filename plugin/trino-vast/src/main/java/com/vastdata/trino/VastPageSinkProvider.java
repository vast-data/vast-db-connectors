/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.inject.Inject;
import com.vastdata.ListShuffler;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.metrics.BufferedInsertMetrics;
import com.vastdata.client.metrics.ByColumnInserterMetrics;
import com.vastdata.client.metrics.RecordBatchSplitterMetrics;
import com.vastdata.trino.VastModule.InsertBufferAllocator;
import com.vastdata.trino.metrics.PageSinkMetrics;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.TypeOperators;

import java.net.URI;
import java.util.List;

import static com.vastdata.trino.VastSessionProperties.getDataEndpoints;
import static com.vastdata.trino.VastSessionProperties.getSeedForShufflingEndpoints;

public class VastPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final VastClient client;
    private final VastConfig config;
    private final InsertBufferAllocator insertBuffersAllocator;
    private final TypeOperators typeOperators;
    private final RecordBatchSplitterMetrics globalRecordBatchSplitterMetrics;
    private final BufferedInsertMetrics globalVastBufferedInsertMetrics;
    private final ByColumnInserterMetrics globalByColumnInserterMetrics;
    private final PageSinkMetrics pageSinkMetrics;
    private final VastIoExecutor vastIoExecutor;
    private final VastCpuExecutor vastCpuExecutor;

    @Inject
    public VastPageSinkProvider(VastClient client,
                                VastConfig config,
                                InsertBufferAllocator insertBuffersAllocator,
                                RecordBatchSplitterMetrics globalRecordBatchSplitterMetrics,
                                BufferedInsertMetrics globalVastBufferedInsertMetrics,
                                ByColumnInserterMetrics globalByColumnInserterMetrics,
                                PageSinkMetrics pageSinkMetrics,
                                VastIoExecutor vastIoExecutor,
                                VastCpuExecutor vastCpuExecutor)
    {
        this.client = client;
        this.config = config;

        this.insertBuffersAllocator = insertBuffersAllocator;

        this.typeOperators = TypeUtils.TYPE_OPERATORS;
        this.globalRecordBatchSplitterMetrics = globalRecordBatchSplitterMetrics;
        this.globalVastBufferedInsertMetrics = globalVastBufferedInsertMetrics;
        this.globalByColumnInserterMetrics = globalByColumnInserterMetrics;
        this.pageSinkMetrics = pageSinkMetrics;
        this.vastIoExecutor = vastIoExecutor;
        this.vastCpuExecutor = vastCpuExecutor;
    }

    public static List<URI> getShuffledDataEndpoints(ConnectorSession session)
    {
        ListShuffler<URI> listShuffler = new ListShuffler<>(
                getSeedForShufflingEndpoints(session));
        return listShuffler.randomizeList(getDataEndpoints(session));
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle,
                                            ConnectorSession session,
                                            ConnectorOutputTableHandle outputTableHandle,
                                            ConnectorPageSinkId pageSinkId)
    {
        return new VastPageSink(client, config, session,
                (VastTransactionHandle) transactionHandle,
                (VastInsertTableHandle) outputTableHandle,
                getShuffledDataEndpoints(session), pageSinkId,
                insertBuffersAllocator.allocator(), typeOperators,
                globalRecordBatchSplitterMetrics,
                globalVastBufferedInsertMetrics, globalByColumnInserterMetrics,
                pageSinkMetrics, vastIoExecutor, vastCpuExecutor);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle,
                                            ConnectorSession session,
                                            ConnectorInsertTableHandle insertTableHandle,
                                            ConnectorPageSinkId pageSinkId)
    {
        return new VastPageSink(client, config, session,
                (VastTransactionHandle) transactionHandle,
                (VastInsertTableHandle) insertTableHandle,
                getShuffledDataEndpoints(session), pageSinkId,
                insertBuffersAllocator.allocator(), typeOperators,
                globalRecordBatchSplitterMetrics,
                globalVastBufferedInsertMetrics, globalByColumnInserterMetrics,
                pageSinkMetrics, vastIoExecutor, vastCpuExecutor);
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle,
                                              ConnectorSession session,
                                              ConnectorMergeTableHandle mergeHandle,
                                              ConnectorPageSinkId pageSinkId)
    {
        return new VastMergeSink(client, config, session,
                (VastTransactionHandle) transactionHandle,
                (VastMergeTableHandle) mergeHandle, pageSinkId,
                insertBuffersAllocator.allocator(),
                globalVastBufferedInsertMetrics,
                getShuffledDataEndpoints(session), vastIoExecutor,
                vastCpuExecutor, typeOperators);
    }
}
