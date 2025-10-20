/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.statistics;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestPerColumnStatsBuilder
{
    @Test
    public void testGetNullFractionEmptyTable()
    {
        double nullFraction = new PerColumnStatsBuilder(0L).getNullFraction(100.0);
        assertEquals(nullFraction, 0.0);
        nullFraction = new PerColumnStatsBuilder(0L).getNullFraction(0.0);
        assertEquals(nullFraction, 0.0);
    }

    @Test
    public void testGetNullFractionTableWithRows()
    {
        double nullFraction = new PerColumnStatsBuilder(10000L).getNullFraction(10000.0);
        assertEquals(nullFraction, 0.0);
        nullFraction = new PerColumnStatsBuilder(10000L).getNullFraction(0.0);
        assertEquals(nullFraction, 1.0);
        nullFraction = new PerColumnStatsBuilder(10000L).getNullFraction(3555.0);
        assertEquals(nullFraction, 0.6445);
    }
}
