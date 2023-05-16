/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.error;

public class VastSerializationException
        extends VastRuntimeException
{
    VastSerializationException(String msg, Throwable rootCause)
    {
        super(msg, rootCause, ErrorType.GENERAL);
    }
}
