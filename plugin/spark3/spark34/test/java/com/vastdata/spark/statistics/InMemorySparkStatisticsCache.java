/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import com.vastdata.client.stats.VastStatisticsStorage;
import com.vastdata.spark.VastTable;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemorySparkStatisticsCache
        implements VastStatisticsStorage<VastTable, Statistics>
{
    private final ConcurrentHashMap<VastTable, Statistics> statisticsMap = new ConcurrentHashMap<>();

    @Override
    public Optional<Statistics> getTableStatistics(VastTable table)
    {
        return Optional.ofNullable(statisticsMap.get(table));
    }

    @Override
    public void setTableStatistics(VastTable table, Statistics tableStatistics)
    {
        statisticsMap.put(table, tableStatistics);
    }

    @Override
    public void deleteTableStatistics(VastTable table)
    {
        statisticsMap.remove(table);
    }
}
