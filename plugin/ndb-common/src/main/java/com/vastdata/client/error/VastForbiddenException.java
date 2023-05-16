/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.error;

public class VastForbiddenException
        extends VastUserException
{
    public VastForbiddenException(String msg)
    {
        super(msg);
    }
}
