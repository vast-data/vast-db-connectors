/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

public interface CompletionListener
{
    void completed(CompletedWriteExecutionComponent phase);
}
