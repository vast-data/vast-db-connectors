/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastClientForTests;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference$;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.mockito.Mockito;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import spark.sql.catalog.ndb.VastCatalogUtils;

import java.util.function.Supplier;

import static com.vastdata.spark.VastDelete.DELETE_UNSUPPORTED_ERROR;
import static org.testng.Assert.assertTrue;

@Listeners(CommonSparkTestUtils.TestListener.class)
public class TestVastDelete
{
    private static VastDelete getVastDelete()
    {
        Supplier<VastClient> supplier = () -> VastClientForTests.getVastClient(
                new JettyHttpClient(), 456);
        StructField charNField = new StructField("c", new CharType(7), true,
                Metadata.empty());
        StructType schema = new StructType(new StructField[] {charNField});
        VastCatalogUtils vastCatalogUtils = Mockito.mock(
                VastCatalogUtils.class);
        VastTable table = new VastTable(vastCatalogUtils, "buck/schem", "tab",
                "id", schema, supplier, false, true);
        return new VastDelete(table, vastCatalogUtils, supplier);
    }

    @Test
    public void testCanDeleteWherePositive()
    {
        VastDelete unit = getVastDelete();
        Predicate equalsCharnNOnlyAscii = new Predicate("=", new Expression[] {
                FieldReference$.MODULE$.apply("c"),
                LiteralValue.apply(UTF8String.fromString("aaa"),
                        DataTypes.StringType)});
        assertTrue(
                unit.canDeleteWhere(new Predicate[] {equalsCharnNOnlyAscii}));
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = DELETE_UNSUPPORTED_ERROR + ".*")
    public void testCanDeleteWhereNegative()
    {
        VastDelete unit = getVastDelete();
        Predicate equalsCharnNNonAscii = new Predicate("=", new Expression[] {
                FieldReference$.MODULE$.apply("c"),
                LiteralValue.apply(UTF8String.fromString("ávch"),
                        DataTypes.StringType)});
        unit.canDeleteWhere(new Predicate[] {equalsCharnNNonAscii});
    }
}
