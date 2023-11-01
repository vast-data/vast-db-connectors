/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.statistics;

import com.vastdata.client.stats.VastStatisticsStorage;
import io.trino.spi.statistics.TableStatistics;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStatistics
        implements VastStatisticsStorage<String, TableStatistics>
{
    private final ConcurrentHashMap<String, TableStatistics> statisticsMap = new ConcurrentHashMap<>();

    @Override
    public Optional<TableStatistics> getTableStatistics(String tableUrl)
    {
        return Optional.ofNullable(statisticsMap.get(tableUrl));
    }

    @Override
    public void setTableStatistics(String tableUrl, TableStatistics tableStatistics)
    {
        statisticsMap.put(tableUrl, tableStatistics);
    }

    @Override
    public void deleteTableStatistics(String tableUrl)
    {
        statisticsMap.remove(tableUrl);
    }
}
