/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.VastVersion;
import com.vastdata.client.error.ErrorType;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastRuntimeException;
import io.airlift.log.Logger;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;

public class VastTrinoExceptionFactory
{
    private static final Logger LOG = Logger.get(VastTrinoExceptionFactory.class);

    private ErrorCodeSupplier fromVastErrorType(ErrorType errorType)
    {
        LOG.info("fromVastErrorType system versions: system=%s, hash=%s", VastVersion.SYS_VERSION, VastVersion.HASH);
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
        LOG.info("fromThrowable system versions: system=%s, hash=%s", VastVersion.SYS_VERSION, VastVersion.HASH);
        return new TrinoException(GENERIC_INTERNAL_ERROR, t);
    }
}
