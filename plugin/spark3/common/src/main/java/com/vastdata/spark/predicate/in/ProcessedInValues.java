/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.predicate.in;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ProcessedInValues<T>
{
    private final T min;
    private final T max;
    private final boolean isFullRange;
    private final int nullCount;

    ProcessedInValues(T min, T max, boolean isFullRange, int nullCount)
    {
        this.min = min;
        this.max = max;
        this.isFullRange = isFullRange;
        this.nullCount = nullCount;
    }

    public int getNullCount()
    {
        return nullCount;
    }

    public boolean isFullRange()
    {
        return isFullRange;
    }

    public T getMin()
    {
        return min;
    }

    public T getMax()
    {
        return max;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("min", min)
                .add("max", max)
                .add("isFullRange", isFullRange)
                .add("nullCount", nullCount)
                .toString();
    }
}
