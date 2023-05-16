/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.error;

import static java.lang.String.format;

public class WorkReachedMaxRetries
        extends VastException
{
    WorkReachedMaxRetries(int currentRetryCount)
    {
        super(format("Number of attempts exceeded configuration: %s", currentRetryCount));
    }

    @Override
    public ErrorType getErrorType()
    {
        return ErrorType.SERVER;
    }
}
