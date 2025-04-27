/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.vdb.sdk;

import com.vastdata.client.CommonRequestHeadersBuilder;
import com.vastdata.client.ValidSchemaNamePredicate;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastDependenciesFactory;
import com.vastdata.client.VastRequestHeadersBuilder;
import com.vastdata.client.stats.StatisticsUrlExtractor;
import io.airlift.configuration.ConfigDefaults;
import io.airlift.http.client.HttpClientConfig;

import java.util.function.Predicate;

public class VastSdkDependenciesFactory
        implements VastDependenciesFactory
{
    public static final String VAST_SDK_V_1 = "VastDbJavaSdk.v1";
    private final VastConfig vastConfig;
    private final ValidSchemaNamePredicate schemaNamePredicate = new ValidSchemaNamePredicate();

    public VastSdkDependenciesFactory(VastConfig vastConfig)
    {
        this.vastConfig = vastConfig;
    }

    @Override
    public Predicate<String> getSchemaNameValidator()
    {
        return schemaNamePredicate;
    }

    @Override
    public VastRequestHeadersBuilder getHeadersFactory()
    {
        return new CommonRequestHeadersBuilder(() -> VAST_SDK_V_1 + vastConfig.getEngineVersion());
    }

    @Override
    public ConfigDefaults<HttpClientConfig> getHttpClientConfigConfigDefaults()
    {
        return null;
    }

    @Override
    public String getConnectorVersionedStatisticsTag()
    {
        return VAST_SDK_V_1;
    }

    @Override
    public StatisticsUrlExtractor<?> getStatisticsUrlHelper()
    {
        return null;
    }
}
