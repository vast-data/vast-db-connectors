/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.importdata;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.testng.Assert.assertEquals;

public class TestImportDataResponseParser
{
    @Test(dataProvider = "importDataResultsProvider")
    public void testAccept(String importDataResponse, int totalFiles, int successfulFiles, int failedFiles)
    {
        ImportDataResponseMapConsumer mapConsumer = new ImportDataResponseMapConsumer();
        AtomicInteger iterationsCtr = new AtomicInteger(0);
        Consumer<Map> decorator = m -> {
            iterationsCtr.incrementAndGet();
            mapConsumer.accept(m);
        };
        ImportDataResponseParser unit = new ImportDataResponseParser(decorator);
        unit.accept(new ByteArrayInputStream(importDataResponse.getBytes(StandardCharsets.UTF_8)));
        assertEquals(iterationsCtr.get(), totalFiles);
        assertEquals(mapConsumer.getResult().getSuccessCount(), successfulFiles);
        assertEquals(mapConsumer.getResult().getFailCount(), failedFiles);
    }

    @DataProvider
    public Object[][] importDataResultsProvider()
    {
        return new Object[][] {
                new Object[] {"{ \"bucket_name\":\"import-data-bucket\", \"object_name\":\"file2.parquet\", \"res\":\"Success\" }{ \"bucket_name\":\"import-data-bucket\", \"object_name\":\"file1.parquet\", \"res\":\"TabularInProgress\" }{ \"bucket_name\":\"import-data-bucket\", \"object_name\":\"file1.parquet\", \"res\":\"Success\" }", 3, 2, 0},
                new Object[] {"{ \"bucket_name\":\"import-data-bucket\", \"object_name\":\"file2.parquet\", \"res\":\"TabularInProgress\" }{ \"bucket_name\":\"import-data-bucket\", \"object_name\":\"file2.parquet\", \"res\":\"Success\" }{ \"bucket_name\":\"import-data-bucket\", \"object_name\":\"file1.parquet\", \"res\":\"TabularInProgress\" }{ \"bucket_name\":\"import-data-bucket\", \"object_name\":\"file1.parquet\", \"res\":\"Fail\" }", 4, 1, 1},
                new Object[] {"{ \"bucket_name\":\"import-data-bucket\", \"object_name\":\"file2.parquet\", \"res\":\"Fail\" }{ \"bucket_name\":\"import-data-bucket\", \"object_name\":\"file1.parquet\", \"res\":\"Fail\" }", 2, 0, 2}
        };
    }
}
