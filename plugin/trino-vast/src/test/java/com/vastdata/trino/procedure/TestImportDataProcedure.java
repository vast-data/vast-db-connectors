/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.procedure;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import io.trino.spi.TrinoException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

public class TestImportDataProcedure
{
    private final VastClient client = new VastClient(null, new VastConfig(), null);

    @Test(expectedExceptions = TrinoException.class)
    public void testImportDataFailsOnNullDst()
    {
        new ImportDataProcedure(client, null).importData(null, null, List.of("col1"), Map.of("b/file", List.of("val1")), null);
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testImportDataFailsOnEmptyDst()
    {
        new ImportDataProcedure(client, null).importData(null, null, List.of("col1"), Map.of("b/file", List.of("val1")), "");
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testImportDataFailsOnNullCol()
    {
        new ImportDataProcedure(client, null).importData(null, null, null, Map.of("b/file", List.of("val1")), "adest");
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testImportDataFailsOnEmptyCol()
    {
        new ImportDataProcedure(client, null).importData(null, null, List.of(""), Map.of("b/file", List.of("val1")), "adest");
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testImportDataFailsOnInvalidDestURL()
    {
        new ImportDataProcedure(client, null).importData(null, null, List.of("col1"), Map.of("b/file", List.of("val1")), "/bucket/schema");
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testImportDataFailsOnInvalidSrcURL()
    {
        new ImportDataProcedure(client, null).importData(null, null, List.of("col1"), Map.of("", List.of("val1")), "/bucket/schema/table");
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testImportDataFailsOnInvalidDefaultValuesListSize()
    {
        new ImportDataProcedure(client, null).importData(null, null, List.of("col1"), Map.of("filename", List.of("val1", "val2")), "/bucket/schema/table");
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testImportDataFailsOnInvalidDefaultValue()
    {
        new ImportDataProcedure(client, null).importData(null, null, List.of("col1"), Map.of("filename", List.of("")), "/bucket/schema/table");
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testImportDataFailsOnEmptyDefaultValue()
    {
        new ImportDataProcedure(client, null).importData(null, null, List.of("col1"), Map.of("filename", List.of()), "/bucket/schema/table");
    }
}
