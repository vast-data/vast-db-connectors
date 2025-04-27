package com.vastdata.vdb.sdk;

import com.vastdata.client.executor.VastRetryConfig;


public record RetryConfig(int maxRetries, int sleepDurationMillis)
{
    VastRetryConfig toVastRetryConfig()
    {
        return new VastRetryConfig(maxRetries, sleepDurationMillis);
    }
}
