/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.metrics;

import org.weakref.jmx.Managed;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class ByColumnInserterMetrics
        implements VastMetrics<ByColumnInserterMetrics>

{
    private static final double NEARLY_FULL_RPC = 0.9;

    protected final RunningStats insertRpcBodyStats = new RunningStats();
    protected final LongAdder insertRpcNearlyFullCount = new LongAdder();

    protected final RunningStats updateRpcBodyStats = new RunningStats();
    protected final LongAdder updateRpcNearlyFullCount = new LongAdder();

    protected final LongAdder insertTimingNanos = new LongAdder();
    protected final LongAdder insertTimingCount = new LongAdder();

    protected final LongAdder updateTimingNanos = new LongAdder();
    protected final LongAdder updateTimingCount = new LongAdder();

    protected final LongAdder totalUpdateTimingPerInsertNanos = new LongAdder();
    protected final LongAdder totalUpdateTimingPerInsertCount = new LongAdder();

    protected final ConcurrentHashMap<String, Long> endpointUsageCounter = new ConcurrentHashMap<>();

    public void recordEndpointUsage(String endpoint)
    {
        endpointUsageCounter.merge(endpoint, 1L, Long::sum);
    }

    public void recordInsertSerialized(long sizeInBytes,
            long maxRequestBodySize)
    {
        insertRpcBodyStats.add(sizeInBytes);
        if (sizeInBytes >= maxRequestBodySize * NEARLY_FULL_RPC) {
            insertRpcNearlyFullCount.increment();
        }
    }

    public void recordUpdateSerialized(long sizeInBytes,
            long maxRequestBodySize)
    {
        updateRpcBodyStats.add(sizeInBytes);
        if (sizeInBytes >= maxRequestBodySize * NEARLY_FULL_RPC) {
            updateRpcNearlyFullCount.increment();
        }
    }

    public void recordInsertTime(long nanos)
    {
        insertTimingNanos.add(nanos);
        insertTimingCount.increment();
    }

    public void recordUpdateTime(long nanos)
    {
        updateTimingNanos.add(nanos);
        updateTimingCount.increment();
    }

    public void recordTotalUpdateTimingPerInsert(long nanos)
    {
        totalUpdateTimingPerInsertNanos.add(nanos);
        totalUpdateTimingPerInsertCount.increment();
    }

    // Insert RPC Metrics
    @Managed
    public long getInsertRpcBodySizeBytesTotal()
    {
        return insertRpcBodyStats.getSum();
    }

    @Managed
    public long getInsertRpcCounts()
    {
        return insertRpcBodyStats.getCount();
    }

    @Managed
    public long getInsertRpcBodySizeBytesSumOfSquares()
    {
        return insertRpcBodyStats.getSumOfSquares();
    }

    @Managed
    public long getInsertRpcNearlyFullCount()
    {
        return insertRpcNearlyFullCount.sum();
    }

    // Update RPC Metrics
    @Managed
    public long getUpdateRpcBodySizeBytesTotal()
    {
        return updateRpcBodyStats.getSum();
    }

    @Managed
    public long getUpdateRpcCounts()
    {
        return updateRpcBodyStats.getCount();
    }

    @Managed
    public long getUpdateRpcBodySizeBytesSumOfSquares()
    {
        return updateRpcBodyStats.getSumOfSquares();
    }

    @Managed
    public long getUpdateRpcNearlyFullCount()
    {
        return updateRpcNearlyFullCount.sum();
    }

    // Timing Metrics
    @Managed
    public long getInsertTimingNanos()
    {
        return insertTimingNanos.sum();
    }

    @Managed
    public long getInsertTimingCount()
    {
        return insertTimingCount.sum();
    }

    @Managed
    public long getUpdateTimingNanos()
    {
        return updateTimingNanos.sum();
    }

    @Managed
    public long getUpdateTimingCount()
    {
        return updateTimingCount.sum();
    }

    @Managed
    public long getTotalUpdateTimingPerInsertNanos()
    {
        return totalUpdateTimingPerInsertNanos.sum();
    }

    @Managed
    public long getTotalUpdateTimingPerInsertCount()
    {
        return totalUpdateTimingPerInsertCount.sum();
    }

    @Managed
    public int getUniqueEndpointsUsed()
    {
        return endpointUsageCounter.size();
    }

    @Managed
    public long getSumEndpointsUsageSum()
    {
        return endpointUsageCounter
                .values()
                .stream()
                .mapToLong(Long::longValue)
                .sum();
    }

    @Managed
    public long getSumEndpointsUsageSumOfSquares()
    {
        return endpointUsageCounter
                .values()
                .stream()
                .mapToLong(v -> v * v)
                .sum();
    }

    @Override
    public void merge(ByColumnInserterMetrics other)
    {
        if (other == null || other == this) {
            return;
        }
        this.insertRpcBodyStats.merge(other.insertRpcBodyStats);
        this.insertRpcNearlyFullCount.add(other.insertRpcNearlyFullCount.sum());
        this.updateRpcBodyStats.merge(other.updateRpcBodyStats);
        this.updateRpcNearlyFullCount.add(other.updateRpcNearlyFullCount.sum());

        this.insertTimingNanos.add(other.insertTimingNanos.sum());
        this.insertTimingCount.add(other.insertTimingCount.sum());
        this.updateTimingNanos.add(other.updateTimingNanos.sum());
        this.updateTimingCount.add(other.updateTimingCount.sum());
        this.totalUpdateTimingPerInsertNanos.add(
                other.totalUpdateTimingPerInsertNanos.sum());
        this.totalUpdateTimingPerInsertCount.add(
                other.totalUpdateTimingPerInsertCount.sum());
    }

    @Override
    public Map<String, Long> asMap()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("insertRpcBodySizeBytesTotal",
                getInsertRpcBodySizeBytesTotal());
        map.put("insertRpcCounts", getInsertRpcCounts());
        map.put("insertRpcBodySizeBytesSumOfSquares",
                getInsertRpcBodySizeBytesSumOfSquares());
        map.put("insertRpcNearlyFullCount", getInsertRpcNearlyFullCount());
        map.put("updateRpcBodySizeBytesTotal",
                getUpdateRpcBodySizeBytesTotal());
        map.put("updateRpcCounts", getUpdateRpcCounts());
        map.put("updateRpcBodySizeBytesSumOfSquares",
                getUpdateRpcBodySizeBytesSumOfSquares());
        map.put("updateRpcNearlyFullCount", getUpdateRpcNearlyFullCount());

        map.put("insertTimingNanos", getInsertTimingNanos());
        map.put("insertTimingCount", getInsertTimingCount());
        map.put("updateTimingNanos", getUpdateTimingNanos());
        map.put("updateTimingCount", getUpdateTimingCount());
        map.put("totalUpdateTimingPerInsertNanos",
                getTotalUpdateTimingPerInsertNanos());
        map.put("totalUpdateTimingPerInsertCount",
                getTotalUpdateTimingPerInsertCount());
        return map;
    }

    @Override
    public Map<String, Long> durationMetrics()
    {
        return new HashMap<>();
    }

    @Override
    public Map<String, Long> diffMetrics()
    {
        return asMap();
    }

    @Override
    public ByColumnInserterMetrics copy()
    {
        ByColumnInserterMetrics clone = new ByColumnInserterMetrics();
        clone.merge(this);
        return clone;
    }
}
