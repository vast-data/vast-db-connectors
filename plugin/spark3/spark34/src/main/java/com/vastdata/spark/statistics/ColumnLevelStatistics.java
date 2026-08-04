/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import com.google.common.base.MoreObjects;
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.connector.read.colstats.Histogram;
import org.apache.spark.sql.connector.read.colstats.HistogramBin;
import scala.Option;
import scala.math.BigInt;

import java.util.Optional;
import java.util.OptionalLong;

public class ColumnLevelStatistics
        implements ColumnStatistics
{
    private final OptionalLong distinctCount;
    private final Optional<Object> min;
    private final Optional<Object> max;
    private final OptionalLong nullCount;
    private final OptionalLong avgLen;
    private final OptionalLong maxLen;
    private final Optional<Histogram> histogram;

    public ColumnLevelStatistics(ColumnStat stat)
    {
        distinctCount = convertOptionBigInt(stat.distinctCount());
        min = convertOptionObject(stat.min());
        max = convertOptionObject(stat.max());
        nullCount = convertOptionBigInt(stat.nullCount());
        avgLen = convertOptionLong(stat.avgLen());
        maxLen = convertOptionLong(stat.maxLen());
        histogram = convertHistogram(stat.histogram());
    }

    public ColumnLevelStatistics(OptionalLong distinctCount,
            Optional<Object> min, Optional<Object> max, OptionalLong nullCount,
            OptionalLong avgLen, OptionalLong maxLen, Optional<Histogram> histogram)
    {
        this.distinctCount = distinctCount;
        this.min = min;
        this.max = max;
        this.nullCount = nullCount;
        this.avgLen = avgLen;
        this.maxLen = maxLen;
        this.histogram = histogram;
    }

    private static OptionalLong convertOptionBigInt(Option<BigInt> value)
    {
        if (value.isDefined()) {
            return OptionalLong.of(value.get().longValue());
        }
        else {
            return OptionalLong.empty();
        }
    }

    private static OptionalLong convertOptionLong(Option<Object> value)
    {
        if (value.isDefined()) {
            return OptionalLong.of((Long) value.get());
        }
        else {
            return OptionalLong.empty();
        }
    }

    private static Optional<Object> convertOptionObject(Option<Object> value)
    {
        if (value.isDefined()) {
            return Optional.of(value.get());
        }
        else {
            return Optional.empty();
        }
    }

    private static Optional<Histogram> convertHistogram(
            Option<org.apache.spark.sql.catalyst.plans.logical.Histogram> histOption)
    {
        if (!histOption.isDefined()) {
            return Optional.empty();
        }
        org.apache.spark.sql.catalyst.plans.logical.Histogram catalystHist = histOption.get();
        return Optional.of(new Histogram() {
            @Override
            public double height()
            {
                return catalystHist.height();
            }

            @Override
            public HistogramBin[] bins()
            {
                org.apache.spark.sql.catalyst.plans.logical.HistogramBin[] catalystBins = catalystHist.bins();
                HistogramBin[] result = new HistogramBin[catalystBins.length];
                for (int i = 0; i < catalystBins.length; i++) {
                    final org.apache.spark.sql.catalyst.plans.logical.HistogramBin bin = catalystBins[i];
                    result[i] = new HistogramBin() {
                        @Override
                        public double lo()
                        {
                            return bin.lo();
                        }

                        @Override
                        public double hi()
                        {
                            return bin.hi();
                        }

                        @Override
                        public long ndv()
                        {
                            return bin.ndv();
                        }
                    };
                }
                return result;
            }
        });
    }

    @Override
    public OptionalLong distinctCount()
    {
        return distinctCount;
    }

    @Override
    public Optional<Object> min()
    {
        return min;
    }

    @Override
    public Optional<Object> max()
    {
        return max;
    }

    @Override
    public OptionalLong nullCount()
    {
        return nullCount;
    }

    @Override
    public OptionalLong avgLen()
    {
        return avgLen;
    }

    @Override
    public OptionalLong maxLen()
    {
        return maxLen;
    }

    @Override
    public Optional<Histogram> histogram()
    {
        return this.histogram;
    }

    @Override
    public String toString()
    {
        return MoreObjects
                .toStringHelper(this)
                .add("distinctCount", distinctCount)
                .add("min", min)
                .add("max", max)
                .add("nullCount", nullCount)
                .add("avgLen", avgLen)
                .add("maxLen", maxLen)
                .add("histogram-present", histogram.isPresent())
                .toString();
    }
}
