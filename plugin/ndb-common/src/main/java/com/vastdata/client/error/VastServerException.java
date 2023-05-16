/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.error;

public class VastServerException
        extends VastException
{
    VastServerException(String msg)
    {
        super(getFormat(msg));
    }

    VastServerException(String msg, Throwable rootCause)
    {
        super(getFormat(msg), rootCause);
    }

    private static String getFormat(String msg)
    {
        return String.format("Vast Server Error: %s", msg);
    }

    @Override
    public ErrorType getErrorType()
    {
        return ErrorType.SERVER;
    }
}
