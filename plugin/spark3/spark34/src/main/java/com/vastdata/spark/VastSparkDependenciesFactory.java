/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.annotations.VisibleForTesting;
import com.vastdata.client.CommonRequestHeadersBuilder;
import com.vastdata.client.ValidSchemaNamePredicate;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastDependenciesFactory;
import com.vastdata.client.VastRequestHeadersBuilder;
import com.vastdata.client.VastVersion;
import com.vastdata.client.stats.StatisticsUrlExtractor;
import org.apache.spark.sql.connector.catalog.Table;

import java.util.function.Predicate;

public class VastSparkDependenciesFactory
        implements VastDependenciesFactory
{
    private final VastConfig vastConfig;
    public VastSparkDependenciesFactory(VastConfig vastConfig)
    {
        this.vastConfig = vastConfig;
    }
    @VisibleForTesting protected static final String VAST_SPARK_CLIENT_TAG = "VastSparkPlugin/" + VastVersion.SYS_VERSION;

    private final Predicate<String> schemaNamePredicate = new ValidSchemaNamePredicate();

    @Override
    public Predicate<String> getSchemaNameValidator()
    {
        return schemaNamePredicate;
    }

    @Override
    public VastRequestHeadersBuilder getHeadersFactory()
    {
        return new CommonRequestHeadersBuilder(() -> VAST_SPARK_CLIENT_TAG + "-spark-" + vastConfig.getEngineVersion());
    }

    @Override
    public String getConnectorVersionedStatisticsTag()
    {
        String sparkConnectorVersion = "v1";
        return "VastSparkPlugin." + sparkConnectorVersion;
    }

    @Override
    public StatisticsUrlExtractor<Table> getStatisticsUrlHelper()
    {
        return SparkStatisticsUrlExtractor.instance();
    }
}
