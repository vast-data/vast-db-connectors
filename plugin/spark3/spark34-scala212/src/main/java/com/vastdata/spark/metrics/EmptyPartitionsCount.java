/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.metrics;

import org.apache.spark.sql.connector.metric.CustomSumMetric;

public class EmptyPartitionsCount
        extends CustomSumMetric
{
    @Override
    public String name()
    {
        return "emptyPartitions";
    }

    @Override
    public String description()
    {
        return "total empty read partitions";
    }
}
