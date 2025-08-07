/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.VastClient;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.net.URI;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class TestVastPageSink
{
    @Mock
    VastClient mockClient;
    @Mock
    ConnectorSession session;

    private AutoCloseable autoCloseable;

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

    private VastPageSink createDummyPageSink()
    {
        VastPageSinkProvider pageSinkProvider = new VastPageSinkProvider(mockClient);
        VastTransactionHandle transactionHandle = new VastTransactionHandle(1L);
        VastInsertTableHandle insertTableHandle = new VastInsertTableHandle(null, List.of(), false, false);
        return (VastPageSink) pageSinkProvider.createPageSink(
                transactionHandle,
                session,
                (ConnectorInsertTableHandle) insertTableHandle,
                null);
    }

    @Test
    public void testShuffledDataEndpoints()
    {
        List<URI> uriList = IntStream.range(0, 10).mapToObj(i -> URI.create("uri-" + i)).toList();
        when(session.getProperty("data_endpoints", List.class)).thenReturn(uriList);
        when(session.getProperty("max_rows_per_insert", Integer.class)).thenReturn(1000);
        when(session.getProperty("import_chunk_limit", Integer.class)).thenReturn(1);

        VastPageSink pageSink = createDummyPageSink();
        VastPageSink pageSink2 = createDummyPageSink();
        List<URI> shuffleUriNoSeed0 = pageSink.getShuffledDataEndpoints();
        List<URI> shuffleUriNoSeed1 = pageSink2.getShuffledDataEndpoints();
        assertNotEquals(shuffleUriNoSeed0, shuffleUriNoSeed1);

        when(session.getProperty(eq("seed_for_shuffling_endpoints"), any())).thenReturn(123L);
        VastPageSink pageSink3 = createDummyPageSink();
        VastPageSink pageSink4 = createDummyPageSink();
        List<URI> shufflesUriWithSeed0 = pageSink3.getShuffledDataEndpoints();
        List<URI> shufflesUriWithSeed1 = pageSink4.getShuffledDataEndpoints();
        assertEquals(shufflesUriWithSeed0, shufflesUriWithSeed1);
        assertNotEquals(shufflesUriWithSeed0, uriList);

        when(session.getProperty("seed_for_shuffling_endpoints", Long.class)).thenReturn(987L);
        VastPageSink pageSink5 = createDummyPageSink();
        List<URI> shufflesUriWithSeed2 = pageSink5.getShuffledDataEndpoints();
        assertNotEquals(shufflesUriWithSeed0, shufflesUriWithSeed2);
    }
}
