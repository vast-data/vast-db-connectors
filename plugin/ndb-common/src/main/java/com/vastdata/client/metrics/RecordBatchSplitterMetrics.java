/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.metrics;

import org.weakref.jmx.Managed;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class RecordBatchSplitterMetrics
        implements VastMetrics<RecordBatchSplitterMetrics>
{
    protected final RunningStats serializedBatchesBytes = new RunningStats();
    protected final AtomicLong oversizedBatchAttemptCount = new AtomicLong(0);
    protected final AtomicLong oversizedBatchBytesTotal = new AtomicLong(0);

    protected final LongAdder splitByRowsTotalSizeBytes = new LongAdder();
    protected final LongAdder splitByRowsBodiesCount = new LongAdder();
    protected final LongAdder splitByRowsInvocations = new LongAdder();

    @Managed
    public double getSerializedBatchesBytesAvg()
    {
        return serializedBatchesBytes.getAverage();
    }

    @Managed
    public double getSerializedBatchesBytesVariance()
    {
        return serializedBatchesBytes.getVariance();
    }

    @Managed
    public long getSerializedBatchesSizeBytesTotal()
    {
        return serializedBatchesBytes.getSum();
    }

    @Managed
    public long getSerializedBatchesCount()
    {
        return serializedBatchesBytes.getCount();
    }

    @Managed
    public long getSerializedBatchesSizeBytesSumOfSquares()
    {
        return serializedBatchesBytes.getSumOfSquares();
    }

    @Managed
    public long getOversizedBatchAttemptCount()
    {
        return oversizedBatchAttemptCount.get();
    }

    @Managed
    public long getOversizedBatchBytesTotal()
    {
        return oversizedBatchBytesTotal.get();
    }

    // Split By Rows Metrics
    @Managed
    public long getSplitByRowsTotalSizeBytes()
    {
        return splitByRowsTotalSizeBytes.sum();
    }

    @Managed
    public long getSplitByRowsBodiesCount()
    {
        return splitByRowsBodiesCount.sum();
    }

    @Managed
    public long getSplitByRowsInvocations()
    {
        return splitByRowsInvocations.sum();
    }

    public void recordSplitByRows(int numOfBodies, long totalSizeInBytes)
    {
        splitByRowsInvocations.increment();
        splitByRowsBodiesCount.add(numOfBodies);
        splitByRowsTotalSizeBytes.add(totalSizeInBytes);
    }

    public void recordSerializedBatch(long serializedBatchesSizeInBytes)
    {
        serializedBatchesBytes.add(serializedBatchesSizeInBytes);
    }

    public void recordOversizedBatchAttempt(long oversizedBatchSizeBytes)
    {
        oversizedBatchAttemptCount.incrementAndGet();
        oversizedBatchBytesTotal.addAndGet(oversizedBatchSizeBytes);
    }

    @Override
    public Map<String, Long> asMap()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("serializedBatchesSizeBytesTotal",
                getSerializedBatchesSizeBytesTotal());
        map.put("serializedBatchesCount", getSerializedBatchesCount());
        map.put("serializedBatchesSizeBytesSumOfSquares",
                getSerializedBatchesSizeBytesSumOfSquares());
        map.put("oversizedBatchAttemptCount", getOversizedBatchAttemptCount());
        map.put("oversizedBatchBytesTotal", getOversizedBatchBytesTotal());

        map.put("splitByRowsTotalSizeBytes", getSplitByRowsTotalSizeBytes());
        map.put("splitByRowsBodiesCount", getSplitByRowsBodiesCount());
        map.put("splitByRowsInvocations", getSplitByRowsInvocations());

        return map;
    }

    @Override
    public Map<String, Long> diffMetrics()
    {
        return asMap();
    }

    @Override
    public void merge(RecordBatchSplitterMetrics other)
    {
        this.serializedBatchesBytes.merge(other.serializedBatchesBytes);
        this.oversizedBatchAttemptCount.addAndGet(
                other.oversizedBatchAttemptCount.get());
        this.oversizedBatchBytesTotal.addAndGet(
                other.oversizedBatchBytesTotal.get());

        this.splitByRowsTotalSizeBytes.add(
                other.splitByRowsTotalSizeBytes.sum());
        this.splitByRowsBodiesCount.add(other.splitByRowsBodiesCount.sum());
        this.splitByRowsInvocations.add(other.splitByRowsInvocations.sum());
    }

    @Override
    public RecordBatchSplitterMetrics copy()
    {
        RecordBatchSplitterMetrics clone = new RecordBatchSplitterMetrics();
        clone.merge(this);
        return clone;
    }
}
