/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver;

import com.vastdata.client.ParsedURL;
import com.vastdata.client.error.VastUserException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestParsedURL
{
    @Test
    public void testBaseURL()
            throws VastUserException
    {
        ParsedURL unit = ParsedURL.of("/");
        assertTrue(unit.isBaseUrl());
        assertFalse(unit.isBucketURL());
    }

    @Test
    public void testBucketURL()
            throws VastUserException
    {
        for (String testUrl : new String[] {"/bucket/", "bucket", "bucket/", "/bucket"}) {
            ParsedURL unit = ParsedURL.of(testUrl);
            String failMessage = String.format("Failed for %s", testUrl);
            assertFalse(unit.isBaseUrl(), failMessage);
            assertTrue(unit.isBucketURL(), failMessage);
            assertFalse((unit.isSchemaURL()), failMessage);
            assertFalse((unit.hasTable()), failMessage);
        }
    }

    @Test
    public void testGetSchemaNameFromURL()
            throws VastUserException
    {
        for (String testUrl : new String[] {"/bucket/schema", "bucket/schema"}) {
            ParsedURL unit = ParsedURL.of(testUrl);
            String failMessage = String.format("Failed for %s", testUrl);
            assertFalse(unit.isBaseUrl(), failMessage);
            assertFalse(unit.isBucketURL(), failMessage);
            assertTrue(unit.isSchemaURL(), failMessage);
            assertFalse(unit.hasTable(), failMessage);
            assertEquals(unit.getFullSchemaPath(), "bucket/schema", failMessage);
        }
    }

    @Test
    public void testGetBucketNameFromURL()
            throws VastUserException
    {
        for (String testUrl : new String[] {"/bucket/schema/table", "bucket/schema/table"}) {
            ParsedURL unit = ParsedURL.of(testUrl);
            String failMessage = String.format("Failed for %s", testUrl);
            assertFalse(unit.isBaseUrl(), failMessage);
            assertFalse(unit.isBucketURL(), failMessage);
            assertFalse(unit.isSchemaURL(), failMessage);
            assertTrue(unit.hasTable(), failMessage);
            assertEquals(unit.getFullSchemaPath(), "bucket/schema", failMessage);
            assertEquals(unit.getTableName(), "table", failMessage);
        }
    }

    @Test(expectedExceptions = VastUserException.class, expectedExceptionsMessageRegExp = ".* abucketname]")
    public void testException()
            throws VastUserException
    {
        String url = "/abucketname";
        ParsedURL.of(url).getFullSchemaPath();
    }

    @Test
    public void testCompose()
    {
        String[] urlParts = {"abucketname"};
        assertEquals(ParsedURL.compose(urlParts), "abucketname");
        urlParts = new String[] {"abucketname", "aschema", "nested1", "nested2"};
        assertEquals(ParsedURL.compose(urlParts), "abucketname/aschema/nested1/nested2");
        urlParts = new String[] {};
        assertEquals(ParsedURL.compose(urlParts), "");
    }
}
