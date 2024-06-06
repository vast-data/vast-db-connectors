/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.util;

import org.testng.annotations.Test;

import java.util.Optional;

import static com.vastdata.client.util.NumOfSplitsEstimator.estimateNumberOfSplits;
import static com.vastdata.client.util.NumOfSplitsEstimator.longToDouble;
import static org.testng.Assert.assertEquals;

public class TestNumOfSplitsEstimator
{
    @Test
    public void testEstimateNumberOfSplits()
    {
        int i = estimateNumberOfSplits(() -> 256, () -> 4000000L, () -> 0L, () -> Optional.of(longToDouble(2879987999L)), 0);
        assertEquals(i, 256);
        i = estimateNumberOfSplits(() -> 256, () -> 4000000L, () -> 0L, Optional::empty, 0);
        assertEquals(i, 256);
        i = estimateNumberOfSplits(() -> 256, () -> 4000000L, () -> 0L, () -> Optional.of(longToDouble(61)), 0);
        assertEquals(i, 1);
        i = estimateNumberOfSplits(() -> 256, () -> 4000000L, () -> 0L, () -> Optional.of(longToDouble(4000000L * 2)), 0);
        assertEquals(i, 2);
        i = estimateNumberOfSplits(() -> 256, () -> 4000000L, () -> 0L, () -> Optional.of(longToDouble(4000000L * 2 + 1)), 0);
        assertEquals(i, 3);
        i = estimateNumberOfSplits(() -> 256, () -> 4000000L, () -> 0L, () -> Optional.of(longToDouble(4000000L * 2 - 1)), 0);
        assertEquals(i, 2);
    }
}
