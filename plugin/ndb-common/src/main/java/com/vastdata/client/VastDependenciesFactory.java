/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.vastdata.client.stats.StatisticsUrlExtractor;
import io.airlift.configuration.ConfigDefaults;
import io.airlift.http.client.HttpClientConfig;

import java.util.function.Predicate;

public interface VastDependenciesFactory
{
    Predicate<String> getSchemaNameValidator();

    VastRequestHeadersBuilder getHeadersFactory();

    ConfigDefaults<HttpClientConfig> getHttpClientConfigConfigDefaults();

    String getConnectorVersionedStatisticsTag();

    StatisticsUrlExtractor<?> getStatisticsUrlHelper();
}
