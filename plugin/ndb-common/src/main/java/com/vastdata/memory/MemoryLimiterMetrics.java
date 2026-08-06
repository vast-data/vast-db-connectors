/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.memory;

import com.vastdata.client.metrics.VastMetrics;
import org.weakref.jmx.Managed;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class MemoryLimiterMetrics
        implements VastMetrics<MemoryLimiterMetrics>
{
    protected final LongAdder memoryExceeded = new LongAdder();
    protected final LongAdder memoryAcquired = new LongAdder();
    protected final LongAdder memoryReleased = new LongAdder();
    protected final LongAdder memoryAllocated = new LongAdder();
    protected final LongAdder runningAllocatedSplits = new LongAdder();

    public void incMemoryExceeded()
    {
        memoryExceeded.increment();
    }

    public void incMemoryReleased()
    {
        memoryReleased.increment();
    }

    public void incMemoryAcquired()
    {
        memoryAcquired.increment();
    }

    public void incRunningAllocatedSplits()
    {
        runningAllocatedSplits.increment();
    }

    public void addMemoryAcquired(long val)
    {
        memoryAcquired.add(val);
    }

    public void addRunningAllocatedSplits(long val)
    {
        runningAllocatedSplits.add(val);
    }

    public void addMemoryAllocated(long val)
    {
        memoryAllocated.add(val);
    }

    @Managed
    public long getMemoryExceeded()
    {
        return memoryExceeded.sum();
    }

    @Managed
    public long getMemoryAcquired()
    {
        return memoryAcquired.sum();
    }

    @Managed
    public long getMemoryAllocated()
    {
        return memoryAllocated.sum();
    }

    @Managed
    public long getMemoryReleased()
    {
        return memoryReleased.sum();
    }

    @Managed
    public long getRunningAllocatedSplits()
    {
        return runningAllocatedSplits.sum();
    }

    @Override
    public Map<String, Long> asMap()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("memoryExceeded", getMemoryExceeded());
        map.put("memoryAcquired", getMemoryAcquired());
        map.put("memoryAllocated", getMemoryAllocated());
        map.put("memoryReleased", getMemoryReleased());
        map.put("runningAllocatedQueries", getRunningAllocatedSplits());
        return map;
    }

    @Override
    public Map<String, Long> diffMetrics()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("memoryExceeded", getMemoryExceeded());
        map.put("memoryAcquired", getMemoryAcquired());
        map.put("memoryReleased", getMemoryReleased());
        return map;
    }

    @Override
    public Map<String, Long> stateMetrics()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("memoryAllocated", getMemoryAllocated());
        map.put("runningAllocatedQueries", getRunningAllocatedSplits());
        return map;
    }

    @Override
    public void merge(MemoryLimiterMetrics other)
    {
        if (other == null || other == this) {
            return;
        }
        this.memoryExceeded.add(other.memoryExceeded.sum());
        this.memoryAcquired.add(other.memoryAcquired.sum());
        this.memoryAllocated.add(other.memoryAllocated.sum());
        this.memoryReleased.add(other.memoryReleased.sum());
        this.runningAllocatedSplits.add(other.runningAllocatedSplits.sum());
    }

    @Override
    public MemoryLimiterMetrics copy()
    {
        MemoryLimiterMetrics clone = new MemoryLimiterMetrics();
        clone.memoryExceeded.add(this.memoryExceeded.sum());
        clone.memoryAcquired.add(this.memoryAcquired.sum());
        clone.memoryAllocated.add(this.memoryAllocated.sum());
        clone.memoryReleased.add(this.memoryReleased.sum());
        clone.runningAllocatedSplits.add(this.runningAllocatedSplits.sum());
        return clone;
    }
}
