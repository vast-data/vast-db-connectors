/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.ndb;

import com.vastdata.spark.SparkTestUtils;
import org.apache.spark.sql.SparkSession;
import org.testng.annotations.Test;

public class TestDefaultSource
{
    @Test(enabled = false)
    public void testSql()
    {
        try (SparkSession session = SparkTestUtils.getSession()) {
            session.sql("select * from ndb.bucket.schema.table where b = 222").collect();
        }
    }

    @Test(enabled = false)
    public void testScala()
    {
        try (SparkSession session = SparkTestUtils.getSession()) {
            session.read()
                    .format("ndb").option("table", "").load()
                    .filter("b = 222").select("b", "c");
        }
    }
}
