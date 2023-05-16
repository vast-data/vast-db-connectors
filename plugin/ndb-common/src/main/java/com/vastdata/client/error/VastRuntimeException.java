/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.error;

public class VastRuntimeException
        extends RuntimeException implements VastErrorTypeSupplier
{
    private final ErrorType errorType;

    VastRuntimeException(String msg, ErrorType errorType)
    {
        super(msg);
        this.errorType = errorType;
    }

    VastRuntimeException(Throwable t, ErrorType errorType)
    {
        super(t);
        this.errorType = errorType;
    }

    public VastRuntimeException(String msg, Throwable t, ErrorType errorType)
    {
        super(msg, t);
        this.errorType = errorType;
    }

    @Override
    public ErrorType getErrorType()
    {
        return errorType;
    }
}
