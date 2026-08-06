/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.partition;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class MockVastPageSink
        implements ConnectorPageSink
{
    private static final Logger LOG = Logger.get(MockVastPageSink.class);

    public record SinkAndPartition(int sinkId, Long partitionKeyHash)
    {}

    public static final ConcurrentHashMap<SinkAndPartition, Integer> PARTITION_DATA = new ConcurrentHashMap<>();

    private final int sinkId;
    private final PartitionKeyHashFunction hashFunction;

    public MockVastPageSink(int sinkId, PartitionKeyHashFunction hashFunction)
    {
        this.sinkId = sinkId;
        this.hashFunction = hashFunction;
    }

    @Override
    public CompletableFuture<Collection<Slice>> appendPage(Page page)
    {
        for (int position = 0; position < page.getPositionCount(); position++) {
            Long partitionKeyHash = hashFunction.apply(page, position);
            SinkAndPartition key = new SinkAndPartition(this.sinkId,
                    partitionKeyHash);
            PARTITION_DATA.merge(key, 1, Integer::sum);
        }
        return completedFuture(List.of());
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(List.of()); // Return empty fragments
    }

    @Override
    public void abort()
    {
    }
}
