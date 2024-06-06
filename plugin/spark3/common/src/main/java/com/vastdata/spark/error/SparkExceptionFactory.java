/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.error;

import com.vastdata.client.error.VastUserException;

public final class SparkExceptionFactory
{
    private SparkExceptionFactory() {}

    public static VastUserException tableOptionNotProvided()
    {
        return new VastUserException("Table option must be provided");
    }

    public static VastUserException uninitializedConfig()
    {
        return new VastUserException("Module config is not initialized");
    }

    public static VastUserException uninitializedSparkContext()
    {
        return new VastUserException("Can't find active Spark context");
    }
}
