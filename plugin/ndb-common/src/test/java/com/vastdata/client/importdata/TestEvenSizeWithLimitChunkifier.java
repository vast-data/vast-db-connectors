/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.importdata;

import com.vastdata.client.schema.ImportDataContext;
import com.vastdata.client.schema.ImportDataFile;
import org.mockito.Mock;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import static com.vastdata.client.importdata.EvenSizeWithLimitChunkifier.CHUNK_SIZE_LIMIT;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestEvenSizeWithLimitChunkifier
{
    @Mock ImportDataFile mockImportDataFile;
    URI endpoint = URI.create("http://localhost");

    private AutoCloseable autoCloseable;

    @BeforeTest
    public void setup()
    {
        autoCloseable = openMocks(this);
    }

    @AfterTest
    public void tearDown()
            throws Exception
    {
        autoCloseable.close();
    }

    @DataProvider
    public static Object[][] testCases()
    {
        return new Object[][] {
                {1, 1, 1, 1, CHUNK_SIZE_LIMIT},
                {CHUNK_SIZE_LIMIT, 1, 1, CHUNK_SIZE_LIMIT, CHUNK_SIZE_LIMIT},
                {CHUNK_SIZE_LIMIT - 1, 1, 1, CHUNK_SIZE_LIMIT - 1, CHUNK_SIZE_LIMIT},
                {CHUNK_SIZE_LIMIT * 2 + 1, 1, 3, CHUNK_SIZE_LIMIT, CHUNK_SIZE_LIMIT},
                {CHUNK_SIZE_LIMIT, 2, 2, CHUNK_SIZE_LIMIT / 2, CHUNK_SIZE_LIMIT},
                {CHUNK_SIZE_LIMIT + 1, 2, 2, CHUNK_SIZE_LIMIT / 2 + 1, CHUNK_SIZE_LIMIT},
                {CHUNK_SIZE_LIMIT * 2, 2, 2, CHUNK_SIZE_LIMIT, CHUNK_SIZE_LIMIT}, // 2 saturated chunks
                {CHUNK_SIZE_LIMIT * 8 * 2 + 1, 8, 8 * 2 + 1, CHUNK_SIZE_LIMIT, CHUNK_SIZE_LIMIT}, //2 * 8 saturated chunks + 1 smaller chunk
        };
    }

    @Test(dataProvider = "testCases")
    public void testApply(int numOfFiles, int numOfEndpoints, int expectedChunksNumber, int expectedMaxChunkSize, int chunkLimit)
    {
        ImportDataContext ctx = new ImportDataContext(Collections.nCopies(numOfFiles, mockImportDataFile), "dest")
                .withChunkLimit(chunkLimit);
        List<URI> endpoints = Collections.nCopies(numOfEndpoints, endpoint);
        EvenSizeWithLimitChunkifier unit = new EvenSizeWithLimitChunkifier();
        List<ImportDataContext> chunksList = unit.apply(ctx, endpoints);
        verifyChunkedResult(chunksList, expectedChunksNumber, expectedMaxChunkSize);
    }

    private void verifyChunkedResult(List<ImportDataContext> chunksList, int expectedChunksNumber, int expectedMaxChunkSize)
    {
        assertEquals(chunksList.size(), expectedChunksNumber, String.format("%s", chunksList));
        assertTrue(chunksList.stream().allMatch(chunkedContext -> chunkedContext.getSourceFiles().size() <= expectedMaxChunkSize));
    }
}
