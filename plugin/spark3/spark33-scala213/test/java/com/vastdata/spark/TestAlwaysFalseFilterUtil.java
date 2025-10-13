/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.testng.annotations.Test;

import static com.vastdata.spark.AlwaysFalseFilterUtil.getAlwaysFalsePredicates;
import static com.vastdata.spark.AlwaysFalseFilterUtil.isAlwaysFalsePredicate;
import static org.testng.Assert.assertTrue;

public class TestAlwaysFalseFilterUtil
{
    @Test
    public void testRoundTrip()
    {
        assertTrue(isAlwaysFalsePredicate(getAlwaysFalsePredicates()));
    }
}
