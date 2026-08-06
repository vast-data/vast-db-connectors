/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.metrics;

import java.util.Map;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

import static java.lang.String.format;

/**
 * A thread-safe class for tracking running statistics (count, sum, average,
 * variance) for a stream of long values.
 */
public class RunningStats
{
    private final LongAdder count = new LongAdder();
    private final LongAdder sum = new LongAdder();
    private final LongAdder sumOfSquares = new LongAdder();
    private final LongAccumulator max = new LongAccumulator(Long::max,
            Long.MIN_VALUE);
    private final LongAccumulator min = new LongAccumulator(Long::min,
            Long.MAX_VALUE);

    /**
     * Adds a new data point to the statistics.
     *
     * @param value the value to add.
     */
    public void add(long value)
    {
        sum.add(value);
        // Note: This may overflow if `value` is large, matching the behavior of the original implementation.
        sumOfSquares.add(value * value);
        max.accumulate(value);
        min.accumulate(value);
        count.increment();
    }

    public long getCount()
    {
        return count.sum();
    }

    public long getSum()
    {
        return sum.sum();
    }

    public long getSumOfSquares()
    {
        return sumOfSquares.sum();
    }

    public long getMax()
    {
        if (count.sum() == 0) {
            return 0;
        }
        return max.get();
    }

    public long getMin()
    {
        if (count.sum() == 0) {
            return 0;
        }
        return min.get();
    }

    public double getAverage()
    {
        // Note: Not atomic. See variance for details.
        long currentCount = count.sum();
        if (currentCount == 0) {
            return 0.0;
        }
        return (double) sum.sum() / currentCount;
    }

    public double getVariance()
    {
        // Note: This calculation is not atomic. The values for count, sum, and sumOfSquares are
        // read as separate operations. Under high concurrency, this can lead to an inconsistent
        // snapshot and a slightly inaccurate (or even negative) variance. This is a common
        // trade-off for performance in metrics collection.
        long currentCount = count.sum();
        if (currentCount < 2) {
            return 0.0;
        }
        double currentSum = sum.sum();
        double currentSumOfSquares = sumOfSquares.sum();
        double mean = currentSum / currentCount;
        // E[X^2] - (E[X])^2
        return (currentSumOfSquares / currentCount) - (mean * mean);
    }

    /**
     * Merges the statistics from another {@link RunningStats} object into this
     * one.
     *
     * @param other the other statistics object.
     */
    public void merge(RunningStats other)
    {
        this.count.add(other.count.sum());
        this.sum.add(other.sum.sum());
        this.sumOfSquares.add(other.sumOfSquares.sum());
        this.max.accumulate(other.max.get());
        this.min.accumulate(other.min.get());
    }

    /**
     * Creates a deep copy of this {@link RunningStats} object.
     *
     * @return a new RunningStats instance with the same values.
     */
    public RunningStats copy()
    {
        RunningStats clone = new RunningStats();
        clone.count.add(this.count.sum());
        clone.sum.add(this.sum.sum());
        clone.sumOfSquares.add(this.sumOfSquares.sum());
        clone.max.accumulate(this.max.get());
        clone.min.accumulate(this.min.get());
        return clone;
    }

    public void addToMap(Map<String, Long> map, String baseName)
    {
        map.put(format("%s-%s", baseName, "count"), count.sum());
        map.put(format("%s-%s", baseName, "sumOfSquares"), sumOfSquares.sum());
        map.put(format("%s-%s", baseName, "sum"), sum.sum());
        map.put(format("%s-%s", baseName, "max"), max.get());
        map.put(format("%s-%s", baseName, "min"), min.get());
    }
}
