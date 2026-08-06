/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.executor;

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Verify.verify;

public final class RetryStrategyFactory
{
    private RetryStrategyFactory()
    {
    }

    public static RetryStrategy fixedSleepBetweenRetries(int numberOfRetries,
            int sleepDuration)
    {
        verify(numberOfRetries >= 0, "Number of retries can't be negative");
        return new FixedSleepBetweenRetries(
                new VastRetryConfig(numberOfRetries, sleepDuration));
    }

    private static class FixedSleepBetweenRetries
            implements RetryStrategy
    {
        private final VastRetryConfig vastRetryConfig;
        private final AtomicInteger currAttempt = new AtomicInteger(0);

        public FixedSleepBetweenRetries(VastRetryConfig vastRetryConfig)
        {
            this.vastRetryConfig = vastRetryConfig;
        }

        @Override
        public boolean shouldRetry()
        {
            return currAttempt.getAndIncrement() < vastRetryConfig.getMaxRetries();
        }

        @Override
        public int getWaitingPeriodMillis()
        {
            return vastRetryConfig.getSleepDuration();
        }

        @Override
        public int getCurrentRetryCount()
        {
            return currAttempt.get();
        }
    }
}
