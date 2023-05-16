/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.executor;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRetryStrategyFactory
{
    @Test
    public void testFixedNumbersStrategy()
    {
        int numberOfRetries = 2;
        int sleepDuration = 1000;
        RetryStrategy unit = RetryStrategyFactory.fixedSleepBetweenRetries(numberOfRetries, sleepDuration);
        for (int i = 0; i < numberOfRetries; i++) {
            assertTrue(unit.shouldRetry(), String.format("Failed on iteration number %s", i));
            assertEquals(unit.getWaitingPeriodMillis(), sleepDuration);
        }
        assertFalse(unit.shouldRetry());
    }
}
