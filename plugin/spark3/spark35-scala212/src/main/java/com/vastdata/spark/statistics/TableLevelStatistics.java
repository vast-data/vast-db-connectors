/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import com.google.common.base.MoreObjects;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

public class TableLevelStatistics implements Statistics
{
    private final OptionalLong rows;
    private final OptionalLong size;
    private final Map<NamedReference, ColumnStatistics> columnStats;

    public TableLevelStatistics(OptionalLong size, OptionalLong rows, Map<NamedReference, ColumnStatistics> columnStats)
    {
        this.rows = rows;
        this.size = size;
        this.columnStats = columnStats;
    }

    @Override
    public OptionalLong sizeInBytes()
    {
        return this.size;
    }

    @Override
    public OptionalLong numRows()
    {
        return this.rows;
    }

    @Override
    public Map<NamedReference, ColumnStatistics> columnStats()
    {
        return columnStats;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("rows", rows)
                .add("size", size)
                .add("columnStats", columnStats)
                .toString();
    }

    public static TableLevelStatistics of(long size, long rows)
    {
        return new TableLevelStatistics(OptionalLong.of(size), OptionalLong.of(rows), new HashMap<>());
    }
}
