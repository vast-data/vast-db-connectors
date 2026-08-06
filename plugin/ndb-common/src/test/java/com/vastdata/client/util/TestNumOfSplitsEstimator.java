/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.util;

import org.testng.annotations.Test;

import java.util.OptionalLong;

import static com.vastdata.client.util.NumOfSplitsEstimator.SPLITS_IDX;
import static com.vastdata.client.util.NumOfSplitsEstimator.getNumOfSplitsEstimation;
import static org.testng.Assert.assertEquals;

public class TestNumOfSplitsEstimator
{
    @Test
    public void testEstimateNumberOfSplits()
    {
        int[] i = getNumOfSplitsEstimation(OptionalLong.of(2879987999L), 256,
                20, 8, 128 * 1024, 1.0, 4000000L, true);
        assertEquals(i[SPLITS_IDX], 256);
        i = getNumOfSplitsEstimation(OptionalLong.empty(), 256, 20, 8,
                128 * 1024, 1.0, 4000000L, true);
        assertEquals(i[SPLITS_IDX], 256);
        i = getNumOfSplitsEstimation(OptionalLong.of(61), 256, 20, 8,
                128 * 1024, 1.0, 4000000L, true);
        assertEquals(i[SPLITS_IDX], 1);
        i = getNumOfSplitsEstimation(OptionalLong.of(4000000L * 2), 256, 20, 8,
                128 * 1024, 1.0, 4000000L, true);
        assertEquals(i[SPLITS_IDX], 2);
        i = getNumOfSplitsEstimation(OptionalLong.of(4000000L * 2 + 1), 256, 20,
                8, 128 * 1024, 1.0, 4000000L, true);
        assertEquals(i[SPLITS_IDX], 3);
        i = getNumOfSplitsEstimation(OptionalLong.of(4000000L * 2 - 1), 256, 20,
                8, 128 * 1024, 1.0, 4000000L, true);
        assertEquals(i[SPLITS_IDX], 2);
        i = getNumOfSplitsEstimation(OptionalLong.of(0), 256, 20, 8, 128 * 1024,
                1.0, 4000000L, true);
        assertEquals(i[SPLITS_IDX], 1);
    }
}
