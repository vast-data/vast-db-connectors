/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static com.vastdata.client.importdata.VastImportDataMetadataUtils.IMPORT_DATA_TABLE_NAME_SUFFIX;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.getTableNameForAPI;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestVastImportDataHiddenColumn
{
    public static Object[][] contextValues()
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

    @ParameterizedTest
    @MethodSource("contextValues")
    public void testGetTableNameForAPI(String testValue, String expected)
    {
        assertEquals(getTableNameForAPI(testValue), expected);
    }
}
