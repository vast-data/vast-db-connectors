/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.metrics;

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

public final class CustomTaskMetricFactory
{
    private CustomTaskMetricFactory() {}

    public static CustomTaskMetric customTaskMetric(CustomMetric metric, long value)
    {
        return new CustomTaskMetric() {
            @Override
            public String name()
            {
                return metric.name();
            }

            @Override
            public long value()
            {
                return value;
            }
        };
    }
}
