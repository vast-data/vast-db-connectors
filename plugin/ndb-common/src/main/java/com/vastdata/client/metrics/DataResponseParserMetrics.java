/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.metrics;

import org.weakref.jmx.Managed;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class DataResponseParserMetrics
        implements VastMetrics<DataResponseParserMetrics>
{
    protected final AtomicLong readNanos = new AtomicLong(0);
    protected final AtomicLong buildNanos = new AtomicLong(0);
    protected final AtomicLong processNanos = new AtomicLong(0);
    protected final AtomicLong prefillBlocks = new AtomicLong(0);
    protected final AtomicLong totalPositions = new AtomicLong(0);
    protected final AtomicLong totalRequests = new AtomicLong(0);
    protected final AtomicLong zeroRowsResponse = new AtomicLong(0);

    @Managed
    public long getReadNanos()
    {
        return readNanos.get();
    }

    @Managed
    public long getBuildNanos()
    {
        return buildNanos.get();
    }

    @Managed
    public long getProcessNanos()
    {
        return processNanos.get();
    }

    @Managed
    public long getPrefillBlocks()
    {
        return prefillBlocks.get();
    }

    @Managed
    public long getTotalPositions()
    {
        return totalPositions.get();
    }

    @Managed
    public long getTotalRequests()
    {
        return totalRequests.get();
    }

    @Managed
    public long getZeroRowsResponse()
    {
        return zeroRowsResponse.get();
    }

    public void incReadNanos()
    {
        readNanos.incrementAndGet();
    }

    public void incBuildNanos()
    {
        buildNanos.incrementAndGet();
    }

    public void incProcessNanos()
    {
        processNanos.incrementAndGet();
    }

    public void incPrefillBlocks()
    {
        prefillBlocks.incrementAndGet();
    }

    public void incTotalPositions()
    {
        totalPositions.incrementAndGet();
    }

    public void incTotalRequests()
    {
        totalRequests.incrementAndGet();
    }

    public void incZeroRowsResponse()
    {
        zeroRowsResponse.incrementAndGet();
    }

    // Add by given delta
    public void addReadNanos(long delta)
    {
        readNanos.addAndGet(delta);
    }

    public void addBuildNanos(long delta)
    {
        buildNanos.addAndGet(delta);
    }

    public void addProcessNanos(long delta)
    {
        processNanos.addAndGet(delta);
    }

    public void addPrefillColumnNanos(long delta)
    {
        prefillBlocks.addAndGet(delta);
    }

    public void addTotalPositions(long delta)
    {
        totalPositions.addAndGet(delta);
    }

    public void addTotalRequests(long delta)
    {
        totalRequests.addAndGet(delta);
    }

    public void addZeroRowsResponse(long delta)
    {
        zeroRowsResponse.addAndGet(delta);
    }

    @Override
    public void merge(DataResponseParserMetrics other)
    {
        if (other == null || other == this) {
            return;
        }

        readNanos.addAndGet(other.readNanos.get());
        buildNanos.addAndGet(other.buildNanos.get());
        processNanos.addAndGet(other.processNanos.get());
        prefillBlocks.addAndGet(other.prefillBlocks.get());
        totalPositions.addAndGet(other.totalPositions.get());
        totalRequests.addAndGet(other.totalRequests.get());
        zeroRowsResponse.addAndGet(other.zeroRowsResponse.get());
    }

    @Override
    public Map<String, Long> asMap()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("readNanos", getReadNanos());
        map.put("buildNanos", getBuildNanos());
        map.put("processNanos", getProcessNanos());
        map.put("prefillBlocks", getPrefillBlocks());
        map.put("totalPositions", getTotalPositions());
        map.put("totalRequests", getTotalRequests());
        map.put("zeroRowsResponse", getZeroRowsResponse());
        return map;
    }

    @Override
    public Map<String, Long> durationMetrics()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("readNanos", getReadNanos());
        map.put("buildNanos", getBuildNanos());
        map.put("processNanos", getProcessNanos());
        return map;
    }

    @Override
    public Map<String, Long> diffMetrics()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("totalPositions", getTotalPositions());
        map.put("totalRequests", getTotalRequests());
        map.put("zeroRowsResponse", getZeroRowsResponse());
        map.put("prefillBlocks", getPrefillBlocks());
        return map;
    }

    @Override
    public DataResponseParserMetrics copy()
    {
        DataResponseParserMetrics clone = new DataResponseParserMetrics();
        clone.readNanos.set(this.readNanos.get());
        clone.buildNanos.set(this.buildNanos.get());
        clone.processNanos.set(this.processNanos.get());
        clone.prefillBlocks.set(this.prefillBlocks.get());
        clone.totalPositions.set(this.totalPositions.get());
        clone.totalRequests.set(this.totalRequests.get());
        clone.zeroRowsResponse.set(this.zeroRowsResponse.get());
        return clone;
    }
}
