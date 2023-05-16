/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import io.airlift.http.client.HttpClient;

import java.net.URI;

import static java.lang.String.format;

public final class VastClientForTests
{
    public static final int RETRY_MAX_COUNT = 2;

    private VastClientForTests() {}

    public static VastClient getVastClient(HttpClient httpClient, int port)
    {
        VastConfig vastConfig = getMockServerReadyVastConfig(port);
        return new VastClient(httpClient, vastConfig, new DummyDependenciesFactory(vastConfig));
    }

    private static VastConfig getMockServerReadyVastConfig(int port)
    {
        return new VastConfig()
                .setEndpoint(URI.create(format("http://localhost:%d", port)))
                .setRegion("us-east-1")
                .setAccessKeyId("pIX3SzyuQVmdrIVZnyy0")
                .setSecretAccessKey("5c5HqW3cDQsUNg68OlhJmq72TM2nZxcP5lR6D1ps")
                .setRetryMaxCount(RETRY_MAX_COUNT)
                .setEngineVersion("1.2.3")
                .setRetrySleepDuration(1);
    }
}
