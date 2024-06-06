/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

public final class SparkVastStatisticsManagerTestUtil
{
    private SparkVastStatisticsManagerTestUtil() {}

    public static void initInMemoryStatsInstance()
    {
        SparkVastStatisticsManager.initInstance(new InMemorySparkStatisticsCache());
    }
}
