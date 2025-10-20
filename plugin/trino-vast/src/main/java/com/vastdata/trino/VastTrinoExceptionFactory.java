/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.error.ErrorType;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastRuntimeException;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;

public class VastTrinoExceptionFactory
{
    private ErrorCodeSupplier fromVastErrorType(ErrorType errorType)
    {
        switch (errorType) {
            case USER:
                return GENERIC_USER_ERROR;
            case SERVER:
            case CLIENT:
            case GENERAL:
            default:
                return GENERIC_INTERNAL_ERROR;
        }
    }

    TrinoException fromVastException(VastException vastException)
    {
        return new TrinoException(fromVastErrorType(vastException.getErrorType()), vastException);
    }

    TrinoException fromVastRuntimeException(VastRuntimeException vastException)
    {
        return new TrinoException(fromVastErrorType(vastException.getErrorType()), vastException);
    }

    TrinoException fromThrowable(Throwable t)
    {
        return new TrinoException(GENERIC_INTERNAL_ERROR, t);
    }
}
