/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.vastdata.client.importdata.VastImportDataMetadataUtils.IMPORT_DATA_TABLE_NAME_SUFFIX;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.getTableNameForAPI;
import static org.testng.Assert.assertEquals;

public class TestVastImportDataHiddenColumn
{
    @DataProvider(name = "tableNames")
    public Object[][] contextValues()
    {
        return new Object[][] {
                {"a" + IMPORT_DATA_TABLE_NAME_SUFFIX, "a"},
                {"a " + IMPORT_DATA_TABLE_NAME_SUFFIX, "a"},
                {"a b" + IMPORT_DATA_TABLE_NAME_SUFFIX, "a b"},
                {"a." + IMPORT_DATA_TABLE_NAME_SUFFIX.stripLeading(), "a." + IMPORT_DATA_TABLE_NAME_SUFFIX.stripLeading()},
                {"a", "a"},
                {"   " + IMPORT_DATA_TABLE_NAME_SUFFIX, ""}
        };
    }

    @Test(dataProvider = "tableNames")
    public void testGetTableNameForAPI(String testValue, String expected)
    {
        assertEquals(getTableNameForAPI(testValue), expected);
    }
}
