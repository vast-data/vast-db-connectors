/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TrinoSerializableTableStatistics
{
    private final Estimate rowCount;
    private final List<Entry<ColumnHandle, ColumnStatistics>> pairList;

    public TrinoSerializableTableStatistics(TableStatistics tableStats)
    {
        this.rowCount = tableStats.getRowCount();
        List<Entry<ColumnHandle, ColumnStatistics>> statsPair = new LinkedList<>();
        tableStats
                .getColumnStatistics()
                .forEach((columnHandle, columnStatistics) -> statsPair.add(
                        new Entry<>(columnHandle, columnStatistics)));
        this.pairList = statsPair;
    }

    @JsonCreator
    public TrinoSerializableTableStatistics(@JsonProperty("rowCount") Estimate rowCount,
                                            @JsonProperty("pairList") List<Entry<ColumnHandle, ColumnStatistics>> pairList)
    {
        this.rowCount = rowCount;
        this.pairList = pairList;
    }

    @JsonProperty
    public Estimate getRowCount()
    {
        return this.rowCount;
    }

    @JsonProperty
    public List<Entry<ColumnHandle, ColumnStatistics>> getPairList()
    {
        return this.pairList;
    }

    @JsonIgnore
    public TableStatistics getTableStatistics()
    {
        Map<ColumnHandle, ColumnStatistics> mapStats = this.pairList
                .stream()
                .collect(Collectors.toMap(Entry::key, Entry::value));
        return new TableStatistics(this.rowCount, mapStats);
    }

    public record Entry<T, T1>(T key,
            T1 value)
    {}
}
