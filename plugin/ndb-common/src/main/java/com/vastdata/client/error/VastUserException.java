/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.error;

public class VastUserException
        extends VastException
{
    public VastUserException(String msg)
    {
        super(msg);
    }

    @Override
    public ErrorType getErrorType()
    {
        return ErrorType.USER;
    }
}
