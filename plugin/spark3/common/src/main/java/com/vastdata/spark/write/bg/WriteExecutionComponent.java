/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

interface WriteExecutionComponent
{
    String name();
    int ordinal();
}
