/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.statistics;

import io.airlift.log.Logger;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;

import java.util.function.BiConsumer;

class PerColumnStatsBuilder
        implements BiConsumer<ColumnStatisticType, Double>
{
    private static final Logger LOG = Logger.get(PerColumnStatsBuilder.class);
    private final ColumnStatistics.Builder builder = new ColumnStatistics.Builder();
    private final long rowCount;
    private Double max;
    private Double min;

    PerColumnStatsBuilder(long rowCount)
    {
        this.rowCount = rowCount;
    }

    ColumnStatistics build()
    {
        if (this.min != null && this.max != null) {
            this.builder.setRange(new DoubleRange(min, max));
        }
        else if (this.min != null) {
            this.builder.setRange(new DoubleRange(min, Double.MAX_VALUE));
        }
        else if (this.max != null) {
            this.builder.setRange(new DoubleRange(Double.MIN_VALUE, max));
        }
        return this.builder.build();
    }

    @Override
    public void accept(ColumnStatisticType statisticType, Double value)
    {
        switch (statisticType) {
            case NUMBER_OF_DISTINCT_VALUES:
                builder.setDistinctValuesCount(Estimate.of(value));
                break;
            case MAX_VALUE:
                this.max = value;
                break;
            case MIN_VALUE:
                this.min = value;
                break;
            case NUMBER_OF_NON_NULL_VALUES:
                double fraction = getNullFraction(value);
                builder.setNullsFraction(Estimate.of(fraction));
                break;
            case TOTAL_SIZE_IN_BYTES:
                builder.setDataSize(Estimate.of(value));
                break;
            default:
                throw new IllegalArgumentException(String.format("Unsupported column statistic type %s", statisticType));
        }
    }

    protected double getNullFraction(Double value)
    {
        return (rowCount == 0L) ? 0.0 : (((double) rowCount - value)) / rowCount;
    }
}
