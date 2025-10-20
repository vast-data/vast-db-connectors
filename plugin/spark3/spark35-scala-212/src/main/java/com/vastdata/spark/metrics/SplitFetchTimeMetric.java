/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.metrics;

import org.apache.spark.sql.connector.metric.CustomSumMetric;

public class SplitFetchTimeMetric
        extends CustomSumMetric
{
    @Override
    public String name()
    {
        return "fetchTime";
    }

    @Override
    public String description()
    {
        return "total page fetch time";
    }
}
