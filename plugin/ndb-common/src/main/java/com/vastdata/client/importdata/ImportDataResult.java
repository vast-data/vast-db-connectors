/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.importdata;

import java.util.Optional;

public class ImportDataResult
{
    private final int successCount;
    private final int failCount;
    private final Optional<String> errorDetails;

    public ImportDataResult(int successCount, int failCount, Optional<String> errorDetails)
    {
        this.successCount = successCount;
        this.failCount = failCount;
        this.errorDetails = errorDetails;
    }

    public int getSuccessCount()
    {
        return successCount;
    }

    public int getFailCount()
    {
        return failCount;
    }

    public Optional<String> getErrorDetails()
    {
        return errorDetails;
    }
}
