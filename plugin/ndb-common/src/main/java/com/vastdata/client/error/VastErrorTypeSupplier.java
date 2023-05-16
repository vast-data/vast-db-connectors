/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.error;

@FunctionalInterface
public interface VastErrorTypeSupplier
{
    ErrorType getErrorType();
}
