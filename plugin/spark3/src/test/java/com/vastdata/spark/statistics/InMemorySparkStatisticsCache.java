/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import com.vastdata.client.stats.VastStatisticsStorage;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.apache.spark.sql.connector.catalog.Table;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemorySparkStatisticsCache
        implements VastStatisticsStorage<Table, Statistics>
{
    private final ConcurrentHashMap<Table, Statistics> statisticsMap = new ConcurrentHashMap<>();

    @Override
    public Optional<Statistics> getTableStatistics(Table table)
    {
        return Optional.ofNullable(statisticsMap.get(table));
    }

    @Override
    public void setTableStatistics(Table table, Statistics tableStatistics)
    {
        statisticsMap.put(table, tableStatistics);
    }

    @Override
    public void deleteTableStatistics(Table table)
    {
        statisticsMap.remove(table);
    }
}
