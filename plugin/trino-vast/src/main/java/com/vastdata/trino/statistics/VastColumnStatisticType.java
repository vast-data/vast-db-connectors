/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.statistics;

import io.trino.spi.expression.FunctionName;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ColumnStatisticType;

import java.util.Optional;

public enum VastColumnStatisticType
{
    MIN_VALUE(new FunctionName("min")),
    MAX_VALUE(new FunctionName("max")),
    NUMBER_OF_DISTINCT_VALUES(new FunctionName("approx_distinct")),
    NUMBER_OF_NON_NULL_VALUES(new FunctionName("count")),
    TOTAL_SIZE_IN_BYTES(ColumnStatisticType.TOTAL_SIZE_IN_BYTES),
    /**/;

    private final Optional<ColumnStatisticType> columnStatisticType;
    private final Optional<FunctionName> aggregationName;

    VastColumnStatisticType(ColumnStatisticType columnStatisticType)
    {
        this.columnStatisticType = Optional.of(columnStatisticType);
        this.aggregationName = Optional.empty();
    }

    VastColumnStatisticType(FunctionName aggregationName)
    {
        this.columnStatisticType = Optional.empty();
        this.aggregationName = Optional.of(aggregationName);
    }

    public ColumnStatisticMetadata createColumnStatisticMetadata(String columnName)
    {
        String connectorAggregationId = name();
        if (columnStatisticType.isPresent()) {
            return new ColumnStatisticMetadata(columnName, connectorAggregationId, columnStatisticType.get());
        }
        return new ColumnStatisticMetadata(columnName, connectorAggregationId, aggregationName.orElseThrow());
    }

    public static VastColumnStatisticType from(ColumnStatisticMetadata columnStatisticMetadata)
    {
        return VastColumnStatisticType.valueOf(columnStatisticMetadata.getConnectorAggregationId());
    }
}
