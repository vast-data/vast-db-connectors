/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import com.vastdata.client.stats.VastStatisticsStorage;
import com.vastdata.client.error.VastUserException;
import com.vastdata.spark.VastTable;
import ndb.NDB;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;

import java.util.Optional;

public class SparkVastStatisticsManager implements VastStatisticsStorage<VastTable, Statistics>
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

    protected static synchronized void initInstance(VastStatisticsStorage<VastTable, Statistics> statisticsStorage)
    {
        instance = new SparkVastStatisticsManager(statisticsStorage);
    }

    private final VastStatisticsStorage<VastTable, Statistics> storage;

    private SparkVastStatisticsManager(VastStatisticsStorage<VastTable, Statistics> storage) {
        this.storage = storage;
    }

    @Override
    public Optional<Statistics> getTableStatistics(VastTable table)
    {
        return storage.getTableStatistics(table);
    }

    @Override
    public void setTableStatistics(VastTable table, Statistics tableStatistics)
    {
        storage.setTableStatistics(table, tableStatistics);
    }

    @Override
    public void deleteTableStatistics(VastTable table)
    {
        storage.deleteTableStatistics(table);
    }
}
