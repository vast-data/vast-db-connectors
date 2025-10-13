/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

interface CompletionListener
{
    void completed(CompletedWriteExecutionComponent phase);
}
