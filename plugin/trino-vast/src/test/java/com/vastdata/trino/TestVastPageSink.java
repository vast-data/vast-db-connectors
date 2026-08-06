/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.metrics.BufferedInsertMetrics;
import com.vastdata.client.metrics.ByColumnInserterMetrics;
import com.vastdata.client.metrics.RecordBatchSplitterMetrics;
import com.vastdata.trino.VastModule.InsertBufferAllocator;
import com.vastdata.trino.metrics.PageSinkMetrics;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.mockito.Mock;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.vastdata.trino.VastSessionProperties.INSERT_BUFFER_MAX_REQUEST_BODY_SIZE;
import static com.vastdata.trino.VastSessionProperties.INSERT_BUFFER_OPEN_VSR_COUNT_PREALLOCATION;
import static com.vastdata.trino.VastSessionProperties.INSERT_BUFFER_OPEN_VSR_TARGET_ROW_COUNT;
import static com.vastdata.trino.VastSessionProperties.INSERT_BUFFER_SIZE_SOFT_LIMIT_IN_BYTES;
import static com.vastdata.trino.VastSessionProperties.INSERT_BUFFER_TARGET_ROW_COUNT_PER_PARTITION_FLUSH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

@TestInstance(Lifecycle.PER_CLASS)
public class TestVastPageSink
{
    @Mock VastClient mockClient;
    @Mock ConnectorSession session;
    @Mock ConnectorPageSinkId pageSinkId;
    @Mock VastTableHandle tableHandle;
    @Mock RecordBatchSplitterMetrics recordBatchSplitterMetrics;
    @Mock ByColumnInserterMetrics byColumnInserterMetrics;
    @Mock BufferedInsertMetrics vastBufferedInsertMetrics;
    @Mock PageSinkMetrics pageSinkMetrics;

    private AutoCloseable autoCloseable;
    private VastIoExecutor ioExecutor;
    private VastCpuExecutor cpuExecutor;
    private VastConfig config;

    @BeforeAll
    public void beforeClass()
    {
        config = new VastConfig();
        ioExecutor = new VastIoExecutor(config);
        cpuExecutor = new VastCpuExecutor(config);
    }

    @AfterAll
    public void afterClass()
    {
        ioExecutor.shutdown();
        cpuExecutor.shutdown();
    }

    @BeforeEach
    public void setup()
    {
        autoCloseable = openMocks(this);
    }

    @AfterEach
    public void tearDown()
            throws Exception
    {
        autoCloseable.close();
    }

    private VastPageSink createDummyPageSink(RootAllocator allocator)
    {
        VastPageSinkProvider pageSinkProvider = new VastPageSinkProvider(
                mockClient, config,
                new InsertBufferAllocator((BufferAllocator) allocator),
                recordBatchSplitterMetrics, vastBufferedInsertMetrics,
                byColumnInserterMetrics, pageSinkMetrics, ioExecutor,
                cpuExecutor);
        VastTransactionHandle transactionHandle = new VastTransactionHandle(1L);
        // VastTableHandle tableHandle = mock(VastTableHandle.class);
        VastInsertTableHandle insertTableHandle = new VastInsertTableHandle(
                tableHandle, List.of(), false, false, Optional.empty());
        return (VastPageSink) pageSinkProvider.createPageSink(transactionHandle,
                session, (ConnectorInsertTableHandle) insertTableHandle,
                pageSinkId);
    }

    @Test
    public void testShuffledDataEndpoints()
    {
        try (RootAllocator allocator = new RootAllocator()) {
            List<URI> uriList = IntStream.range(0, 10).mapToObj(
                    i -> URI.create("uri-" + i)).toList();
            when(session.getProperty("data_endpoints", List.class)).thenReturn(
                    uriList);
            when(session.getProperty("max_rows_per_insert",
                    Integer.class)).thenReturn(1000);
            when(session.getProperty("import_chunk_limit",
                    Integer.class)).thenReturn(1);

            when(session.getProperty(INSERT_BUFFER_OPEN_VSR_COUNT_PREALLOCATION,
                    Integer.class)).thenReturn(100);
            when(session.getProperty(
                    INSERT_BUFFER_TARGET_ROW_COUNT_PER_PARTITION_FLUSH,
                    Integer.class)).thenReturn(100);
            when(session.getProperty(INSERT_BUFFER_SIZE_SOFT_LIMIT_IN_BYTES,
                    Long.class)).thenReturn(100L);
            when(session.getProperty(INSERT_BUFFER_MAX_REQUEST_BODY_SIZE,
                    Long.class)).thenReturn(100L);
            when(session.getProperty(INSERT_BUFFER_OPEN_VSR_TARGET_ROW_COUNT,
                    Integer.class)).thenReturn(1);

            VastPageSink pageSink = createDummyPageSink(allocator);
            VastPageSink pageSink2 = createDummyPageSink(allocator);
            List<URI> shuffleUriNoSeed0 = pageSink.getShuffledDataEndpoints();
            List<URI> shuffleUriNoSeed1 = pageSink2.getShuffledDataEndpoints();
            assertNotEquals(shuffleUriNoSeed0, shuffleUriNoSeed1);

            when(session.getProperty(eq("seed_for_shuffling_endpoints"),
                    any())).thenReturn(123L);
            VastPageSink pageSink3 = createDummyPageSink(allocator);
            VastPageSink pageSink4 = createDummyPageSink(allocator);
            List<URI> shufflesUriWithSeed0 = pageSink3.getShuffledDataEndpoints();
            List<URI> shufflesUriWithSeed1 = pageSink4.getShuffledDataEndpoints();
            assertEquals(shufflesUriWithSeed0, shufflesUriWithSeed1);
            assertNotEquals(shufflesUriWithSeed0, uriList);

            when(session.getProperty("seed_for_shuffling_endpoints",
                    Long.class)).thenReturn(987L);
            VastPageSink pageSink5 = createDummyPageSink(allocator);
            List<URI> shufflesUriWithSeed2 = pageSink5.getShuffledDataEndpoints();
            assertNotEquals(shufflesUriWithSeed0, shufflesUriWithSeed2);
        }
    }
}
