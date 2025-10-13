/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastClientForTests;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.testng.annotations.Test;

import java.util.function.Supplier;

import static com.vastdata.spark.VastDelete.DELETE_UNSUPPORTED_ERROR;
import static org.testng.Assert.assertTrue;

public class TestVastDelete
{
    @Test
    public void testCanDeleteWherePositive()
    {
        VastDelete unit = getVastDelete();
        Filter equalsCharnNOnlyAscii = new EqualTo("c", "aaa");
        assertTrue(unit.canDeleteWhere(new Filter[] {equalsCharnNOnlyAscii}));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = DELETE_UNSUPPORTED_ERROR + ".*")
    public void testCanDeleteWhereNegative()
    {
        VastDelete unit = getVastDelete();
        Filter equalsCharnNNonAscii = new EqualTo("c", "ávch"); // fails on validity check during serialization
        unit.canDeleteWhere(new Filter[] {equalsCharnNNonAscii});
    }

    private static VastDelete getVastDelete()
    {
        Supplier<VastClient> supplier = () -> VastClientForTests.getVastClient(new JettyHttpClient(), 456);
        StructField charNField = new StructField("c", new CharType(7), true, Metadata.empty());
        StructType schema = new StructType(new StructField[] {charNField});
        VastTable table = new VastTable("buck/schem", "tab", "id", schema, supplier, false);
        return new VastDelete(table, supplier);
    }
}
