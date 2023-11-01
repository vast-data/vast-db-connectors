/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

public interface CompletedWriteExecutionComponent
        extends WriteExecutionComponent
{
    Status status();
}
