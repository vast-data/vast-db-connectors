/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.metrics;

import com.vastdata.client.metrics.VastMetrics;
import org.weakref.jmx.Managed;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class PageSourceMetrics
        implements VastMetrics<PageSourceMetrics>
{
    private final AtomicLong activeSplits = new AtomicLong(0);
    private final AtomicLong blockedByMemory = new AtomicLong(0);
    private final AtomicLong numExceededMaxBytesPerColumn = new AtomicLong(0);

    @Managed
    public long getActiveSplits()
    {
        return activeSplits.get();
    }

    public void incActiveSplits()
    {
        activeSplits.incrementAndGet();
    }

    public void addActiveSplits(long count)
    {
        activeSplits.addAndGet(count);
    }

    @Managed
    public long getBlockedByMemory()
    {
        return blockedByMemory.get();
    }

    public void incBlockedByMemory()
    {
        blockedByMemory.incrementAndGet();
    }

    public void addBlockedByMemory(long count)
    {
        blockedByMemory.addAndGet(count);
    }

    @Managed
    public long getNumExceededMaxBytesPerColumn()
    {
        return numExceededMaxBytesPerColumn.get();
    }

    public void incNumExceededMaxBytesPerColumn()
    {
        numExceededMaxBytesPerColumn.incrementAndGet();
    }

    public void addNumExceededMaxBytesPerColumn(long count)
    {
        numExceededMaxBytesPerColumn.addAndGet(count);
    }

    @Override
    public void merge(PageSourceMetrics other)
    {
        if (other == null || other == this) {
            return;
        }

        activeSplits.addAndGet(other.activeSplits.get());
        blockedByMemory.addAndGet(other.blockedByMemory.get());
        numExceededMaxBytesPerColumn.addAndGet(other.numExceededMaxBytesPerColumn.get());
    }

    @Override
    public Map<String, Long> asMap()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("activeSplits", getActiveSplits());
        map.put("blockedByMemory", getBlockedByMemory());
        map.put("numExceededMaxBytesPerColumn", getNumExceededMaxBytesPerColumn());
        return map;
    }

    public Map<String, Long> diffMetrics()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("blockedByMemory", getBlockedByMemory());
        map.put("numExceededMaxBytesPerColumn", getNumExceededMaxBytesPerColumn());
        return map;
    }

    @Override
    public Map<String, Long> stateMetrics()
    {
        Map<String, Long> map = new HashMap<>();
        if (activeSplits.get() != 0) {
            map.put("activeSplits", getActiveSplits());
        }
        return map;
    }

    @Override
    public PageSourceMetrics copy()
    {
        PageSourceMetrics copy = new PageSourceMetrics();
        copy.activeSplits.set(this.activeSplits.get());
        copy.blockedByMemory.set(this.blockedByMemory.get());
        copy.numExceededMaxBytesPerColumn.set(this.numExceededMaxBytesPerColumn.get());
        return copy;
    }
}
