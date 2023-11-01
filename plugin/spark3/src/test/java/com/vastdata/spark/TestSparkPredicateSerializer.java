/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.spark.sql.types.Decimal;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;

public class TestSparkPredicateSerializer
{
    @Test
    public void testDecimal()
    {
        String expectedString = "[-1, -29, 11, 84, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]"; // taken from trino literal serialization of "99999.99999"
        org.apache.spark.sql.types.Decimal decimal = new Decimal();
        BigDecimal javaBigDecimal = new BigDecimal("99999.99999");
        scala.math.BigDecimal scalaBigDecimal = scala.math.BigDecimal.javaBigDecimal2bigDecimal(javaBigDecimal);
        Decimal set = decimal.set(scalaBigDecimal, 10, 5);
        SparkPredicateSerializer sparkPredicateSerializer = new SparkPredicateSerializer("testDecimal", null, null);
        byte[] result = sparkPredicateSerializer.decimalToByteArray(set);
        assertEquals(Arrays.toString(result), expectedString);
    }
}
