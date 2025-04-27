/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.error;

import com.vastdata.client.VastResponse;
import com.vastdata.client.tx.VastTransaction;

import java.net.URI;
import java.util.Optional;

import static java.lang.String.format;

public final class VastExceptionFactory
{
    private VastExceptionFactory() {}

    public static VastRuntimeException toRuntime(String message, Throwable t)
    {
        if (t instanceof VastException) {
            return new VastRuntimeException(message, t, ((VastException) t).getErrorType());
        }
        else {
            return new VastRuntimeException(message, t, ErrorType.GENERAL);
        }
    }

    public static VastRuntimeException toRuntime(Throwable t)
    {
        if (t instanceof VastException) {
            return new VastRuntimeException(t, ((VastException) t).getErrorType());
        }
        else {
            return new VastRuntimeException(t, ErrorType.GENERAL);
        }
    }

    public static VastUserException conflictException(String msg)
    {
        return new VastConflictException(msg);
    }

    public static VastUserException forbiddenException(String msg)
    {
        return new VastForbiddenException(msg);
    }

    public static VastUserException userException(String msg)
    {
        return new VastUserException(msg);
    }

    public static VastServerException serverException(String msg)
    {
        return new VastServerException(msg);
    }

    public static VastServerException serverException(String msg, Throwable rootCause)
    {
        return new VastServerException(msg, rootCause);
    }

    public static VastSerializationException serializationException(String msg, Exception e)
    {
        return new VastSerializationException(msg, e);
    }

    public static VastIOException ioException(String msg, Throwable e)
    {
        return new VastIOException(msg, e);
    }

    public static VastInvalidServerResponse serverInvalidResponseError(String msg)
    {
        return new VastInvalidServerResponse(msg);
    }

    public static VastClosedTransactionException closedTransaction(VastTransaction transactionHandle)
    {
        return new VastClosedTransactionException(transactionHandle);
    }

    public static Optional<VastException> checkResponseStatus(final VastResponse vastResponse, final String msg)
    {
        final int status = vastResponse.getStatus();
        if (status >= 500) {
            return Optional.of(serverException(renderErrorMessage(wrapErrorMessageWithErrorDetails(msg, status, vastResponse.getRequestUri()), vastResponse)));
        }
        if (status >= 400) {
            switch (status) { // TODO - complete all supported specific codes
                case 409:
                    return Optional.of(conflictException(renderErrorMessage(wrapErrorMessageWithErrorDetails(msg, status, vastResponse.getRequestUri()), vastResponse)));
                default:
                    return Optional.of(userException(renderErrorMessage(wrapErrorMessageWithErrorDetails(msg, status, vastResponse.getRequestUri()), vastResponse)));
            }
        }
        return Optional.empty();
    }

    private static String renderErrorMessage(String msg, VastResponse vastResponse)
    {
        return vastResponse.getErrorMessage().map(err -> format("%s. %s", msg, err)).orElse(msg);
    }

    private static String wrapErrorMessageWithErrorDetails(String msg, int code, URI requestUri)
    {
        return format("%s. request URI: %s. HTTP Error: %s", msg, requestUri, code);
    }

    public static Throwable maxRetries(int currentRetryCount)
    {
        return new WorkReachedMaxRetries(currentRetryCount);
    }

    public static VastRuntimeException tableHandleIdNotFound(String schemaName, String tableName)
    {
        return new VastRuntimeException(format("Failed fetching table handle ID for table: %s/%s", schemaName, tableName), ErrorType.GENERAL);
    }
}
