/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static java.lang.String.format;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestValidSchemaNamePredicate
{
    private final ValidSchemaNamePredicate unit = new ValidSchemaNamePredicate();

    @Test
    public void testValidSchemaName()
    {
        ImmutableList.of("/buck/schema/nested/", "/Buck/Schema/Nested/nested1", "buck/schema", "buck/schema/", "bu ck/sc ema")
                .forEach(name -> {
                    boolean valid = unit.test(name);
                    assertTrue(valid, format("Schema name %s was tested negative", name));
                });
    }

    @Test
    public void testInvalidSchemaName()
    {
        ImmutableList.of("/buck", "/Buck/", "buck", "buck///", "", "/", "////buck/schema", "///buck1A")
                .forEach(name -> {
                    boolean valid = unit.test(name);
                    assertFalse(valid, format("Schema name %s was expected to test negative", name));
                });
    }
}
