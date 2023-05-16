/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.error;

import com.vastdata.client.importdata.ImportDataResult;

public class ImportDataFailure
        extends VastException
{
    public static final String FILES_IMPORT_ERROR_MESSAGE_FORMAT = "Import data result %s: %d files successful, %d files failed";
    public static final String REQUEST_EXECUTION_ERROR_MESSAGE_FORMAT = "Import data request failed with status %d";
    public static final String REQUEST_EXECUTION_ERROR_NOT_FOUND = "Import data request failed: Object not found";
    public static final String REQUEST_EXECUTION_ERROR_PERMISSIONS = "Import data request failed: Incorrect permissions";
    public static final String REQUEST_EXECUTION_ERROR_BAD_REQUEST = "Import data request failed: Bad request";
    private final ErrorType errorType;

    private ImportDataFailure(ImportDataResult result)
    {
        super(String.format(FILES_IMPORT_ERROR_MESSAGE_FORMAT, result.getErrorDetails().orElse(""), result.getSuccessCount(), result.getFailCount()));
        this.errorType = ErrorType.SERVER;
    }

    private ImportDataFailure(int status, ErrorType errorType)
    {
        super(String.format(REQUEST_EXECUTION_ERROR_MESSAGE_FORMAT, status));
        this.errorType = errorType;
    }

    public ImportDataFailure(String msg, ErrorType errorType)
    {
        super(msg);
        this.errorType = errorType;
    }

    public static ImportDataFailure fromSuccessfulRequest(ImportDataResult result)
    {
        return new ImportDataFailure(result);
    }

    public static ImportDataFailure fromFailedRequest(int status)
    {
        switch (status) {
            case 404:
                return new ImportDataFailure(REQUEST_EXECUTION_ERROR_NOT_FOUND, ErrorType.USER);
            case 403:
                return new ImportDataFailure(REQUEST_EXECUTION_ERROR_PERMISSIONS, ErrorType.USER);
            case 400:
                return new ImportDataFailure(REQUEST_EXECUTION_ERROR_BAD_REQUEST, ErrorType.USER);
            default:
                return new ImportDataFailure(status, ErrorType.SERVER);
        }
    }

    @Override
    public ErrorType getErrorType()
    {
        return errorType;
    }
}
