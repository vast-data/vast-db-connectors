/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.executor;

public interface RetryStrategy
{
    boolean shouldRetry();

    int getWaitingPeriodMillis();

    int getCurrentRetryCount();
}
