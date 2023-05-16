/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.error;

import com.fasterxml.jackson.core.JsonProcessingException;

public class VastIOException
        extends VastException
{
    public VastIOException(String msg, Throwable rootCause)
    {
        super(msg, rootCause);
    }

    public VastIOException(JsonProcessingException e) {super(e);}

    @Override
    public ErrorType getErrorType()
    {
        return ErrorType.SERVER;
    }
}
