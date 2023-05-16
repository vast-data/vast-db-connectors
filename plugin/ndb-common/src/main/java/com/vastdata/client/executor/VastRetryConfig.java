/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.executor;

public class VastRetryConfig
{
    private final int maxRetries;
    private final int sleepDuration;

    public VastRetryConfig(int maxRetries, int sleepDuration)
    {
        this.maxRetries = maxRetries;
        this.sleepDuration = sleepDuration;
    }

    public int getMaxRetries()
    {
        return maxRetries;
    }

    public int getSleepDuration()
    {
        return sleepDuration;
    }
}
