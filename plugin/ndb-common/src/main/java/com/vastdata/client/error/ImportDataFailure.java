/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.error;

import com.vastdata.client.VastResponse;
import com.vastdata.client.importdata.ImportDataResult;

import java.net.URI;
import java.util.Optional;

import static java.lang.String.format;

public class ImportDataFailure
        extends VastException
{
    public static final String FILES_IMPORT_ERROR_MESSAGE_FORMAT = "Import data result %s: %d files successful, %d files failed";
    public static final String REQUEST_EXECUTION_ERROR_MESSAGE_FORMAT = "Import data request failed with status %s: Request URI: %s";
    public static final String REQUEST_EXECUTION_ERROR_NOT_FOUND = "Import data request failed: Object not found";
    public static final String REQUEST_EXECUTION_ERROR_PERMISSIONS = "Import data request failed: Incorrect permissions";
    public static final String REQUEST_EXECUTION_ERROR_BAD_REQUEST = "Import data request failed: Bad request";
    private final ErrorType errorType;

    private ImportDataFailure(ImportDataResult result)
    {
        super(format(FILES_IMPORT_ERROR_MESSAGE_FORMAT, result.getErrorDetails().orElse(""), result.getSuccessCount(), result.getFailCount()));
        this.errorType = ErrorType.SERVER;
    }

    private ImportDataFailure(int status, ErrorType errorType, URI requestUri)
    {
        super(format(REQUEST_EXECUTION_ERROR_MESSAGE_FORMAT, status, requestUri));
        this.errorType = errorType;
    }

    public ImportDataFailure(String msg, ErrorType errorType, URI requestUri)
    {
        super(format("%s, Request URI: %s", msg, requestUri));
        this.errorType = errorType;
    }

    public static ImportDataFailure fromSuccessfulRequest(ImportDataResult result)
    {
        return new ImportDataFailure(result);
    }

    public static ImportDataFailure fromFailedRequest(VastResponse response)
    {
        return fromFailedRequest(response.getStatus(), response.getRequestUri(), response.getErrorMessage());
    }

    private static ImportDataFailure fromFailedRequest(int status, URI requestUri, Optional<String> errorMessage)
    {
        switch (status) {
            case 404:
                return new ImportDataFailure(getImportDataFailureMessage(REQUEST_EXECUTION_ERROR_NOT_FOUND, errorMessage), ErrorType.USER, requestUri);
            case 403:
                return new ImportDataFailure(getImportDataFailureMessage(REQUEST_EXECUTION_ERROR_PERMISSIONS, errorMessage), ErrorType.USER, requestUri);
            case 400:
                return new ImportDataFailure(getImportDataFailureMessage(REQUEST_EXECUTION_ERROR_BAD_REQUEST, errorMessage), ErrorType.USER, requestUri);
            default:
                return new ImportDataFailure(status, ErrorType.SERVER, requestUri);
        }
    }

    private static String getImportDataFailureMessage(String errorTypeMessage, Optional<String> extraMessageFromException)
    {
        if (!extraMessageFromException.isPresent()) {
            return errorTypeMessage;
        }
        else {
            return format("%s, Server Error Message: %s", errorTypeMessage, extraMessageFromException);
        }
    }

    @Override
    public ErrorType getErrorType()
    {
        return errorType;
    }
}
