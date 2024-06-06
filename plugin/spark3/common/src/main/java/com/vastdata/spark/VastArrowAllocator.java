/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public final class VastArrowAllocator
{
    private VastArrowAllocator() {}

    private static final RootAllocator rootAllocator = new RootAllocator();
    private static final BufferAllocator writeAllocator = rootAllocator.newChildAllocator("NDBWriteAllocator", 0, Long.MAX_VALUE);

    public static BufferAllocator writeAllocator()
    {
        return writeAllocator;
    }
}
