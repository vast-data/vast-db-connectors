/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import com.vastdata.client.stats.VastStatisticsStorage;
import com.vastdata.client.error.VastUserException;
import ndb.NDB;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.apache.spark.sql.connector.catalog.Table;

import java.util.Optional;

public class SparkVastStatisticsManager implements VastStatisticsStorage<Table, Statistics>
{
    private static SparkVastStatisticsManager instance = null;

    public static SparkVastStatisticsManager getInstance()
    {
        if (instance == null) {
            initDefaultInstance();
        }
        return instance;
    }

    private static synchronized void initDefaultInstance()
    {
        if (instance == null) {
            try {
                initInstance(new SparkPersistentStatistics(NDB.getVastClient(NDB.getConfig()), NDB.getConfig()));
            }
            catch (VastUserException e) {
                throw new RuntimeException("Failed initializing default instance", e);
            }
        }
    }

    protected static synchronized void initInstance(VastStatisticsStorage<Table, Statistics> statisticsStorage)
    {
        instance = new SparkVastStatisticsManager(statisticsStorage);
    }

    private final VastStatisticsStorage<Table, Statistics> storage;

    private SparkVastStatisticsManager(VastStatisticsStorage<Table, Statistics> storage) {
        this.storage = storage;
    }

    @Override
    public Optional<Statistics> getTableStatistics(Table table)
    {
        return storage.getTableStatistics(table);
    }

    @Override
    public void setTableStatistics(Table table, Statistics tableStatistics)
    {
        storage.setTableStatistics(table, tableStatistics);
    }

    @Override
    public void deleteTableStatistics(Table table)
    {
        storage.deleteTableStatistics(table);
    }
}
