/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.metrics;

import com.vastdata.client.metrics.VastMetrics;
import org.weakref.jmx.Managed;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class SplitSourceMetrics
        implements VastMetrics<SplitSourceMetrics>
{
    private final AtomicLong estimateSize = new AtomicLong(0);
    private final AtomicLong initialSplits = new AtomicLong(0);

    @Managed
    public long getEstimateSize()
    {
        return estimateSize.get();
    }

    public void incEstimateSize()
    {
        estimateSize.incrementAndGet();
    }

    public void addEstimateSize(long count)
    {
        estimateSize.addAndGet(count);
    }

    @Managed
    public long getInitialSplits()
    {
        return initialSplits.get();
    }

    public void incInitialSplits()
    {
        initialSplits.incrementAndGet();
    }

    public void addInitialSplits(long count)
    {
        initialSplits.addAndGet(count);
    }

    @Override
    public void merge(SplitSourceMetrics other)
    {
        if (other == null || other == this) {
            return;
        }

        estimateSize.addAndGet(other.estimateSize.get());
        initialSplits.addAndGet(other.initialSplits.get());
    }

    @Override
    public Map<String, Long> asMap()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("estimateSize", getEstimateSize());
        map.put("initialSplits", getInitialSplits());
        return map;
    }

    public Map<String, Long> diffMetrics()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("estimateSize", getEstimateSize());
        map.put("initialSplits", getInitialSplits());
        return map;
    }

    @Override
    public SplitSourceMetrics copy()
    {
        SplitSourceMetrics copy = new SplitSourceMetrics();
        copy.estimateSize.set(this.estimateSize.get());
        copy.initialSplits.set(this.initialSplits.get());
        return copy;
    }
}
