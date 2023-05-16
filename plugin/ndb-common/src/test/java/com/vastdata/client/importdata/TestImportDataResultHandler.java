/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.importdata;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.vastdata.client.RequestsHeaders;
import com.vastdata.client.VastResponse;
import com.vastdata.client.error.ImportDataFailure;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
import io.airlift.http.client.HeaderName;
import org.mockito.Mock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static com.vastdata.client.importdata.ImportDataResultHandler.handleResponse;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestImportDataResultHandler
{
    @Mock VastResponse mockImportDataResponse;
    @Mock VastResponse mockStartTransactionResponse;
    @Mock VastTraceToken mockTraceToken;
    @Mock VastTransaction mockTrans;

    @Test(dataProvider = "importDataResultsProvider")
    public void testSuccessfulRequest(int expectedNumberOfSuccessfulFiles, int expectedNumberOfFailedFiles, boolean expectedException)
            throws ImportDataFailure
    {
        initMocks(this);
        when(mockTraceToken.toString()).thenReturn("SomeTraceToken");
        when(mockTrans.getId()).thenReturn(Long.parseUnsignedLong("514026084031791104"));
        Multimap<HeaderName, String> m = ImmutableMultimap.of(HeaderName.of(RequestsHeaders.TABULAR_TRANSACTION_ID.getHeaderName()), "transid");
        when(mockStartTransactionResponse.getHeaders()).thenReturn(m);
        when(mockStartTransactionResponse.getStatus()).thenReturn(200);
        byte[] bytes = new byte[0];
        when(mockImportDataResponse.getBytes()).thenReturn(bytes);
        when(mockImportDataResponse.getStatus()).thenReturn(200);
        try {
            assertTrue(handleResponse(mockImportDataResponse, new ImportDataResult(expectedNumberOfSuccessfulFiles, expectedNumberOfFailedFiles, Optional.empty()), mockTraceToken));
        }
        catch (ImportDataFailure e) {
            if (!expectedException) {
                throw e;
            }
            assertEquals(e.getMessage(), String.format(ImportDataFailure.FILES_IMPORT_ERROR_MESSAGE_FORMAT, "", expectedNumberOfSuccessfulFiles, expectedNumberOfFailedFiles));
        }
    }

    @DataProvider
    public Object[][] importDataResultsProvider()
    {
        return new Object[][] {
            new Object[] {2, 0, false},
            new Object[] {1, 1, true},
            new Object[] {0, 2, true},
        };
    }

    @Test(expectedExceptions = ImportDataFailure.class)
    public void testFailedRequest()
            throws ImportDataFailure
    {
        initMocks(this);
        when(mockTraceToken.toString()).thenReturn("SomeTraceToken");
        when(mockImportDataResponse.getBytes()).thenReturn("test fail message".getBytes(StandardCharsets.UTF_8));
        when(mockImportDataResponse.getStatus()).thenReturn(400);
        handleResponse(mockImportDataResponse, null, mockTraceToken);
    }
}
