package com.vastdata.vdb.sdk;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import io.airlift.http.client.HttpClient;

public record VastSdk(VastConfig config, VastClient client, RetryConfig retryConfig)
{
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_SLEEP_DURATION_MILLIS = 10;

    public VastSdk(HttpClient httpClient, VastSdkConfig config)
    {
        this(httpClient, config, new RetryConfig(DEFAULT_MAX_RETRIES, DEFAULT_SLEEP_DURATION_MILLIS));
    }

    public VastSdk(HttpClient httpClient, VastSdkConfig config, RetryConfig retryConfig)
    {
        this(config.getVastConfig(),
                new VastClient(httpClient, config.getVastConfig(), new VastSdkDependenciesFactory(config.getVastConfig())),
                retryConfig);
    }

    public VastClient getVastClient()
    {
        return client;
    }

    public Table getTable(String schemaName, String tableName)
    {
        return new Table(schemaName, tableName, client, config.getDataEndpoints(), retryConfig);
    }
}
