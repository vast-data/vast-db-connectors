/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.metrics;

import org.apache.spark.sql.connector.metric.CustomAvgMetric;

public class PageSizeAVG extends CustomAvgMetric
{
    @Override
    public String name()
    {
        return "avgPageSize";
    }

    @Override
    public String description()
    {
        return "avg non-empty scan page size in bytes";
    }
}
