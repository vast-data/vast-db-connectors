/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.vastdata.client.error.VastUserException;

public final class VerifyParam
{
    private VerifyParam() {}

    public static void verify(boolean condition, String msg)
            throws VastUserException
    {
        if (!condition) {
            throw new VastUserException(msg);
        }
    }
}
