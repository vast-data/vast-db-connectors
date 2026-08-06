/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.vastdata.client.TableSpecifiers.IMPORT_DATA_SPECIFIER;
import static com.vastdata.client.TableSpecifiers.NON_ACID_SPECIFIER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTableSpecifiers
{
    @Test
    public void testSimpleTableName()
    {
        TableSpecifiers parser = TableSpecifiers.parse("my_table");
        assertEquals(parser.getTableName(), "my_table");
        assertFalse(parser.hasOperationSpecifiers());
        assertFalse(parser.isForImportDataOperation());
        assertFalse(parser.isForNonAcidOperation());
    }

    @Test
    public void testImportDataSpecifier()
    {
        TableSpecifiers parser = TableSpecifiers.parse(
                "my_table " + IMPORT_DATA_SPECIFIER);
        assertEquals(parser.getTableName(), "my_table");
        assertTrue(parser.hasOperationSpecifiers());
        assertTrue(parser.isForImportDataOperation());
        assertFalse(parser.isForNonAcidOperation());
    }

    @Test
    public void testNonAcidSpecifier()
    {
        TableSpecifiers parser = TableSpecifiers.parse(
                "my_table " + NON_ACID_SPECIFIER);
        assertEquals(parser.getTableName(), "my_table");
        assertTrue(parser.hasOperationSpecifiers());
        assertFalse(parser.isForImportDataOperation());
        assertTrue(parser.isForNonAcidOperation());
    }

    @Test
    public void testMultipleSpecifiers()
    {
        // Order 1
        TableSpecifiers p1 = TableSpecifiers.parse(
                "my_table " + IMPORT_DATA_SPECIFIER + " " + NON_ACID_SPECIFIER);
        assertEquals(p1.getTableName(), "my_table");
        assertTrue(p1.isForImportDataOperation());
        assertTrue(p1.isForNonAcidOperation());

        // Order 2
        TableSpecifiers p2 = TableSpecifiers.parse(
                "my_table " + NON_ACID_SPECIFIER + " " + IMPORT_DATA_SPECIFIER);
        assertEquals(p2.getTableName(), "my_table");
        assertTrue(p2.isForImportDataOperation());
        assertTrue(p2.isForNonAcidOperation());
    }

    @DataProvider(name = "legacyLogicalEquivalents")
    public Object[][] legacyLogicalEquivalents()
    {
        return new Object[][] {
                // 1. "a" + SUFFIX
                {"a " + IMPORT_DATA_SPECIFIER, "a", true, true},
                // 2. "a " + SUFFIX (Extra spacing before specifier - parser strips space during loop)
                {"a  " + IMPORT_DATA_SPECIFIER, "a", true, true},
                // 3. "a b" + SUFFIX
                {"a b " + IMPORT_DATA_SPECIFIER, "a b", true, true},
                // 4. "a." + SUFFIX.stripLeading() (No preceding space - should NOT be treated as specifier)
                {"a." + IMPORT_DATA_SPECIFIER,
                        "a." + IMPORT_DATA_SPECIFIER,
                        false,
                        false},
                // 5. "a"
                {"a", "a", false, false}};
    }

    @Test(dataProvider = "legacyLogicalEquivalents")
    public void testLegacyEquivalents(String input, String expectedTableName,
            boolean expectedImportFlag, boolean expectHasFlags)
    {
        TableSpecifiers parser = TableSpecifiers.parse(input);
        assertEquals(parser.getTableName(), expectedTableName);
        assertEquals(parser.isForImportDataOperation(), expectedImportFlag);
        assertEquals(parser.hasOperationSpecifiers(), expectHasFlags);
    }

    @Test
    public void testWhitespaceOnlySpecifierInput()
    {
        // Legacy test: {"   " + IMPORT_DATA_TABLE_NAME_SUFFIX, ""}
        // In the new logic, " vast.import_data" trims down to "vast.import_data"
        // Since it doesn't *end with* " vast.import_data" (missing leading space after trim), it won't parse as a specifier.
        TableSpecifiers parser = TableSpecifiers.parse(
                "   " + IMPORT_DATA_SPECIFIER);
        assertEquals(parser.getTableName(), IMPORT_DATA_SPECIFIER);
        assertFalse(parser.isForImportDataOperation());
    }

    // --- Exceptional / Edge Cases ---

    @Test
    public void testParseToSimpleOrThrow()
            throws Exception
    {
        Exception customException = new IllegalArgumentException(
                "Not a simple table");

        // Should return cleanly
        assertEquals(TableSpecifiers.parseUnspecifiedTableName("my_table",
                customException), "my_table");

        // Should throw the provided exception
        assertThatThrownBy(() -> TableSpecifiers.parseUnspecifiedTableName(
                "my_table " + IMPORT_DATA_SPECIFIER, customException)).isSameAs(
                customException);
    }

    @Test
    public void testNullInput()
    {
        assertThatThrownBy(() -> TableSpecifiers.parse(null)).isInstanceOf(
                NullPointerException.class);
    }
}
