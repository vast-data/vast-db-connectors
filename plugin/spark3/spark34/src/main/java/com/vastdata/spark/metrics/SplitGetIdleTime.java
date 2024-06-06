/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.metrics;

import org.apache.spark.sql.connector.metric.CustomSumMetric;

public class SplitGetIdleTime
        extends CustomSumMetric
{
    @Override
    public String name()
    {
        return "idleGetTime";
    }

    @Override
    public String description()
    {
        return "total idle time before GET";
    }
}
