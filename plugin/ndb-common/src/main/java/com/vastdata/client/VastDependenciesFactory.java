/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import io.airlift.configuration.ConfigDefaults;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.function.Predicate;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public interface VastDependenciesFactory
{
    ConfigDefaults<HttpClientConfig> HTTP_CLIENT_CONFIG_CONFIG_DEFAULTS = cfg -> {
        cfg.setConnectTimeout(new Duration(1, MINUTES));
        cfg.setIdleTimeout(new Duration(3600, SECONDS));
        cfg.setRequestTimeout(new Duration(360000, SECONDS));
        cfg.setMaxConnectionsPerServer(250);
        cfg.setMaxContentLength(DataSize.of(32, MEGABYTE));
        cfg.setSelectorCount(10);
        cfg.setTimeoutThreads(8);
        cfg.setTimeoutConcurrency(4);
    };

    Predicate<String> getSchemaNameValidator();

    VastRequestHeadersBuilder getHeadersFactory();

    default ConfigDefaults<HttpClientConfig> getHttpClientConfigConfigDefaults()
    {
        return HTTP_CLIENT_CONFIG_CONFIG_DEFAULTS;
    }
}
