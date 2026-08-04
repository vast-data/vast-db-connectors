/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.componenttests;

import com.vastdata.client.CommonRequestHeadersBuilder;
import com.vastdata.client.ValidSchemaNamePredicate;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastDependenciesFactory;
import com.vastdata.client.VastRequestHeadersBuilder;
import com.vastdata.client.stats.StatisticsUrlExtractor;
import io.airlift.configuration.ConfigDefaults;
import io.airlift.http.client.HttpClientConfig;

import java.util.function.Predicate;

public class DummyDependenciesFactory
        implements VastDependenciesFactory
{
    private final VastConfig vastConfig;
    private final ValidSchemaNamePredicate schemaNamePredicate = new ValidSchemaNamePredicate();

    DummyDependenciesFactory(VastConfig vastConfig)
    {
        this.vastConfig = vastConfig;
    }

    @Override
    public Predicate<String> getSchemaNameValidator()
    {
        return schemaNamePredicate;
    }

    @Override
    public VastRequestHeadersBuilder getHeadersFactory(final String endUser)
    {
        return new CommonRequestHeadersBuilder(
                () -> "DUMMY-" + vastConfig.getEngineVersion() + "-user:" + endUser);
    }

    @Override
    public ConfigDefaults<HttpClientConfig> getHttpClientConfigConfigDefaults()
    {
        return null;
    }

    @Override
    public String getConnectorVersionedStatisticsTag()
    {
        return "dummy";
    }

    @Override
    public StatisticsUrlExtractor<?> getStatisticsUrlHelper()
    {
        return null;
    }

    @Override
    public String getClientTag()
    {
        return "";
    }
}
