/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import com.vastdata.spark.CommonSparkTestUtils;
import org.testng.annotations.Listeners;

@Listeners(CommonSparkTestUtils.TestListener.class)
public final class SparkVastStatisticsManagerTestUtil
{
    private SparkVastStatisticsManagerTestUtil()
    {
    }

    public static void initInMemoryStatsInstance()
    {
        SparkVastStatisticsManager.initInstance(
                new InMemorySparkStatisticsCache());
    }
}
