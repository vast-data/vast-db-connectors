/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.metrics;

import org.apache.spark.sql.connector.metric.CustomSumMetric;

public class SplitFetchIdleTimeMetric
        extends CustomSumMetric
{
    @Override
    public String name()
    {
        return "idleNextTime";
    }

    @Override
    public String description()
    {
        return "total idle time before NEXT";
    }
}
