/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestValidSchemaNamePredicate
{
    private final ValidSchemaNamePredicate unit = new ValidSchemaNamePredicate();

    @DataProvider
    Object[][] schemaNames()
    {
        return new Object[][] {
                {"/buck/schema/nested/", true},
                {"/Buck/Schema/Nested/nested1", true},
                {"buck/schema", true},
                {"buck/schema/", true},
                {"bu ck/sc ema", true},
                {"/buck", false},
                {"/Buck/", false},
                {"buck", false},
                {"buck///", false},
                {"", false},
                {"/", false},
                {"////buck/schema", false},
                {"///buck1A", false},
        };
    }

    @Test(dataProvider = "schemaNames")
    public void testValidSchemaName(final String schemaName, final boolean isValid)
    {
        assertEquals(unit.test(schemaName), isValid, format("Schema name %s was tested negative", schemaName));
    }
}
