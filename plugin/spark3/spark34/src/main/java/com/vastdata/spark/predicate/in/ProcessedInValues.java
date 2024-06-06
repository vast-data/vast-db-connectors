/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.predicate.in;

import org.apache.spark.sql.connector.expressions.LiteralValue;

import java.util.StringJoiner;

class ProcessedInValues
{
    private final LiteralValue<?> min;
    private final LiteralValue<?> max;
    private final boolean isFullRange;
    private final int nullCount;

    ProcessedInValues(LiteralValue<?> min, LiteralValue<?> max, boolean isFullRange, int nullCount)
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

    public LiteralValue<?> getMin()
    {
        return min;
    }

    public LiteralValue<?> getMax()
    {
        return max;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", ProcessedInValues.class.getSimpleName() + "[", "]")
                .add("min=" + min)
                .add("max=" + max)
                .add("isFullRange=" + isFullRange)
                .add("nullCount=" + nullCount)
                .toString();
    }
}
