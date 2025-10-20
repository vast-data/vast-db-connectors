/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.importdata;

import com.google.common.collect.ImmutableList;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastResponse;
import com.vastdata.client.error.ImportDataFailure;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.client.executor.RetryStrategy;
import com.vastdata.client.executor.RetryStrategyFactory;
import com.vastdata.client.schema.ImportDataContext;
import com.vastdata.client.schema.ImportDataFile;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
import org.mockito.Mock;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class TestImportDataExecutor
{
    private static final int NUMBER_OF_RETRIES = 2;
    @Mock VastClient mockClient;
    @Mock ImportDataContext mockCtx;
    @Mock VastTransaction mockTrans;
    @Mock ImportDataFile mockImportDataFile;
    @Mock VastResponse mockResponse;
    @Mock VastTraceToken mockTraceToken;
    URI uri1 = URI.create("http://localhost:8080");
    URI uri2 = URI.create("http://127.0.0.1:8080");

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

    @Test(expectedExceptions = VastRuntimeException.class, expectedExceptionsMessageRegExp = ".*Number of attempts exceeded configuration: " + (NUMBER_OF_RETRIES + 1))
    public void testExecuteThrowsExceptionOnMaxRetries()
            throws VastException
    {
        testExecute(503);
    }

    @Test(expectedExceptions = ImportDataFailure.class, expectedExceptionsMessageRegExp = ".*" + ImportDataFailure.REQUEST_EXECUTION_ERROR_NOT_FOUND + ".*")
    public void testExecuteThrowsExceptionOnObjectNotFound()
            throws VastException
    {
        testExecute(404);
    }

    @Test(expectedExceptions = ImportDataFailure.class, expectedExceptionsMessageRegExp = ".*" + ImportDataFailure.REQUEST_EXECUTION_ERROR_PERMISSIONS + ".*")
    public void testExecuteThrowsExceptionOnBadPermissions()
            throws VastException
    {
        testExecute(403);
    }

    private void testExecute(int returnCode)
            throws VastException
    {
        when(mockTraceToken.toString()).thenReturn("SomeTraceToken");
        when(mockResponse.getStatus()).thenReturn(returnCode);
        when(mockResponse.getRequestUri()).thenReturn(uri1);
        when(mockResponse.getErrorMessage()).thenReturn(Optional.empty());
        when(mockResponse.getBytes()).thenReturn("dummy".getBytes(StandardCharsets.UTF_8));
        when(mockClient.importData(any(VastTransaction.class), any(VastTraceToken.class), any(ImportDataContext.class), any(), any(URI.class)))
                .thenReturn(mockResponse);
        when(mockCtx.getSourceFiles()).thenReturn(ImmutableList.of(mockImportDataFile));
        when(mockCtx.getDest()).thenReturn("bucket/schema/table");
        when(mockCtx.getChunkLimit()).thenReturn(Optional.empty());
        when(mockImportDataFile.hasSchemaRoot()).thenReturn(Boolean.TRUE);
        ImportDataExecutor<VastTransaction> unit = new ImportDataExecutor<>(mockClient);
        Supplier<RetryStrategy> retryStrategy = this::getRetryStrategy;
        unit.execute(mockCtx, mockTrans, mockTraceToken, ImmutableList.of(uri1, uri2), retryStrategy, true);
    }

    private RetryStrategy getRetryStrategy()
    {
        return RetryStrategyFactory.fixedSleepBetweenRetries(NUMBER_OF_RETRIES, 1);
    }
}
