/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.error;

public class VastInvalidServerResponse
        extends VastRuntimeException
{
    VastInvalidServerResponse(String msg)
    {
        super(msg, ErrorType.SERVER);
    }
}
