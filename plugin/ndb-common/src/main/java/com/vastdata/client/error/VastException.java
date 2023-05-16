/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.error;

public abstract class VastException
        extends Exception implements VastErrorTypeSupplier
{
    VastException(String msg, Throwable rootCause)
    {
        super(msg, rootCause);
    }

    VastException(String msg)
    {
        super(msg);
    }

    public VastException(Exception e) { super(e);}

    public abstract ErrorType getErrorType();
}
