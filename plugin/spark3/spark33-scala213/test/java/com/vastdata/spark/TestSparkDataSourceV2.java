/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestSparkDataSourceV2
{
    @Test(enabled = false)
    public void testSimple()
    {
        try (SparkSession session = SparkTestUtils.getSession()) {
            Dataset<Row> dataset = session
                    .read()
                    .format("com.vastdata.spark.VastTableProvider")
                    .option("table", "VastTableProvider.TESTTABLE_1")
                    .load()
                    .filter("b = 222")
                    .select("b", "c");
            List<Row> rows = dataset.collectAsList();
            assertEquals(rows.size(), 1);
            assertEquals(rows.get(0).getInt(0), 222);
            assertEquals(rows.get(0).getDouble(1), 2.5, 0);
        }
    }
}
