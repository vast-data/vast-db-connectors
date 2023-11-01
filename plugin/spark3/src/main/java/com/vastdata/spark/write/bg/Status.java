/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

public final class Status
{
    private final boolean success;
    private final Throwable throwable;

    public Status(boolean success, Throwable throwable)
    {
        this.success = success;
        this.throwable = throwable;
    }

    public boolean isSuccess() {
        return this.success;
    }

    public Throwable getFailureCause()
    {
        return this.throwable;
    }
}
