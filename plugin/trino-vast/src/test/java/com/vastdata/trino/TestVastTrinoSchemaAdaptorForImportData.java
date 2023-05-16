/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.schema.ImportDataContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.util.Strings;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestVastTrinoSchemaAdaptorForImportData
{
    @DataProvider(name = "contextValues")
    public Object[][] contextValues()
    {
        Optional<Object> empty = Optional.empty();
        Optional<IllegalArgumentException> illegal = Optional.of(new IllegalArgumentException());
        return new Object[][] {
                {List.of(), Map.of("bucket/file", List.of()), "bucket/schema/table", empty},
                {List.of(), Map.of("/bucket/file", List.of()), "/bucket/schema/table", empty},
                {List.of("c1"), Map.of("bucket/file", List.of("aaa"), "/bucket/file", List.of("bbb")), "/bucket/schema/table", empty},
                {List.of(), Map.of(), "/bucket/schema/table", illegal},
                {null, Map.of("bucket/file", List.of("aaa")), "/bucket/schema/table", illegal},
                {List.of(), null, "/bucket/schema/table", illegal},
                {List.of("c1"), Map.of("bucket/file", List.of("aaa")), "", illegal},
                {List.of("c1"), Map.of("bucket/file", List.of("aaa")), null, illegal},
                {List.of(), Map.of("bucket/file", List.of("aaa")), "/bucket/schema/table", illegal},
                {List.of("c1", "c2"), Map.of("bucket/file", List.of("aaa"), "/bucket/file", List.of("bbb")), "/bucket/schema/table", illegal},
                {List.of("c1"), Map.of("bucket/file", List.of("aaa"), "/bucket/file", List.of("bbb", 888)), "/bucket/schema/table", illegal},
        };
    }

    @Test(dataProvider = "contextValues")
    public void testAdaptForImportData(List<String> columns, Map<String, List<String>> filesInfo, String dest, Optional<Exception> expectedException)
    {
        try {
            ImportDataContext unit = new VastTrinoSchemaAdaptor().adaptForImportData(columns, filesInfo, dest);
            unit.getSourceFiles().forEach(importDataFile -> {
                assertFalse(importDataFile.getSrcBucket().startsWith("/"));
                assertFalse(Strings.isNullOrEmpty(importDataFile.getSrcFile()));
            });
        }
        catch (Exception e) {
            assertTrue(expectedException.isPresent(), String.format("Unexpected exception: %s", e));
            assertEquals(e.getClass(), expectedException.get().getClass());
        }
    }
}
