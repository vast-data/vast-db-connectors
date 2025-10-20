/* Copyright (C) Vast Data Ltd. */

package com.vastdata.trino;

import com.vastdata.ListShuffler;
import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.ConnectorSession;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.vastdata.trino.VastSessionProperties.getSeedForShufflingEndpoints;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestVastMergeSink
{
    @Mock
    VastClient mockClient;
    @Mock
    ConnectorSession session;

    private AutoCloseable autoCloseable;

    @BeforeMethod
    public void setup()
    {
        autoCloseable = openMocks(this);
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        autoCloseable.close();
    }

    private VastMergeSink createDummyMergeSink()
    {
        VastPageSinkProvider pageSinkProvider = new VastPageSinkProvider(mockClient);
        VastTransactionHandle transactionHandle = new VastTransactionHandle(1L, true, false);
        VastMergeTableHandle mergeTableHandle = new VastMergeTableHandle(null, List.of());
        return (VastMergeSink) pageSinkProvider.createMergeSink(
                transactionHandle,
                session,
                mergeTableHandle,
                null);
    }

    @Test
    public void testPageDistribution()
            throws VastException
    {
        VastTableHandle tableHandle = new VastTableHandle("buck/schem", "tab", "id", false);
        VastMergeTableHandle mergeTableHandle = new VastMergeTableHandle(tableHandle, List.of());
        List<URI> dataEndPoints = List.of(URI.create("http://127.0.0.0:8080"), URI.create("http://127.0.0.1:8080"), URI.create("http://127.0.0.2:8080"));
        when(session.getProperty("data_endpoints", List.class)).thenReturn(dataEndPoints);
        when(session.getProperty("max_rows_per_delete", Integer.class)).thenReturn(1000);
        ListShuffler<URI> listShuffler = new ListShuffler<>(getSeedForShufflingEndpoints(session));
        VastMergeSink mergeSink = new VastMergeSink(mockClient, session, new VastTransactionHandle(123, true, false), mergeTableHandle, listShuffler.randomizeList(dataEndPoints));

        //Delete page should contain 2 blocks: operation (2 for DELETE) and columnID block
        ByteArrayBlock opBlock = new ByteArrayBlock(1, Optional.empty(), new byte[] {2});
        LongArrayBlock idBlock = new LongArrayBlock(1, Optional.empty(), new long[] {200});
        Block[] mockBlocks = new Block[] {opBlock, idBlock};
        Page page = new Page(mockBlocks);

        for (int i = 0; i < 3; i++) {
            mergeSink.storeMergedRows(page);
        }

        //verify that each end point is called at least once
        for (URI endpoint : dataEndPoints) {
            verify(mockClient, atLeastOnce()).deleteRows(any(), any(), any(), any(), eq(endpoint), any());
        }
    }

    @Test
    public void testRandomDataEndPoints()
    {
        {
            List<URI> uriList = new ArrayList<>();
            IntStream.range(0, 10).mapToObj(i ->
                    uriList.add(URI.create("uri-" + i))).collect(Collectors.toList());
            when(session.getProperty("data_endpoints", List.class)).thenReturn(uriList);
            when(session.getProperty("max_rows_per_insert", Integer.class)).thenReturn(1000);
            when(session.getProperty("import_chunk_limit", Integer.class)).thenReturn(1);

            VastMergeSink mergeSink = createDummyMergeSink();
            VastMergeSink mergeSink1 = createDummyMergeSink();
            List<URI> shuffleUriNoSeed0 = mergeSink.getShuffledDataEndpoints();
            List<URI> shuffleUriNoSeed1 = mergeSink1.getShuffledDataEndpoints();
            assertNotEquals(shuffleUriNoSeed0, shuffleUriNoSeed1);

            when(session.getProperty(eq("seed_for_shuffling_endpoints"), any())).thenReturn(123L);
            VastMergeSink mergeSink2 = createDummyMergeSink();
            VastMergeSink mergeSink3 = createDummyMergeSink();
            List<URI> shufflesUriWithSeed0 = mergeSink2.getShuffledDataEndpoints();
            List<URI> shufflesUriWithSeed1 = mergeSink3.getShuffledDataEndpoints();
            assertEquals(shufflesUriWithSeed0, shufflesUriWithSeed1);
            assertNotEquals(shufflesUriWithSeed0, uriList);

            when(session.getProperty("seed_for_shuffling_endpoints", Long.class)).thenReturn(987L);
            VastMergeSink mergeSink4 = createDummyMergeSink();
            List<URI> shufflesUriWithSeed2 = mergeSink4.getShuffledDataEndpoints();
            assertNotEquals(shufflesUriWithSeed0, shufflesUriWithSeed2);
        }
    }
}
