/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.VastConfig;
import com.vastdata.client.stats.StatisticsUrlExtractor;
import org.apache.spark.sql.connector.catalog.Table;

public class VastSparkDependenciesFactory
        extends VastSparkCommonDependenciesFactory
{

    public VastSparkDependenciesFactory(VastConfig vastConfig)
    {
        super(vastConfig);
    }

    @Override
    public StatisticsUrlExtractor<Table> getStatisticsUrlHelper()
    {
        return SparkStatisticsUrlExtractor.instance();
    }
}
