/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.memory;

import java.util.concurrent.CompletableFuture;

public class MemoryAllocationInfo
{
    private final MemoryAllocationState allocationState;
    private final long memoryRequestAmount;
    private final CompletableFuture<Boolean> future;
    private final long timestamp;

    public MemoryAllocationInfo(MemoryAllocationState allocationState,
                                long memoryRequestAmount,
                                CompletableFuture<Boolean> future)
    {
        this.allocationState = allocationState;
        this.memoryRequestAmount = memoryRequestAmount;
        this.future = future;
        this.timestamp = System.currentTimeMillis();
    }

    public MemoryAllocationState getAllocationState()
    {
        return allocationState;
    }

    public CompletableFuture<Boolean> getFuture()
    {
        return future;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public long getMemoryRequestAmount()
    {
        return memoryRequestAmount;
    }

    public enum MemoryAllocationState
    {
        ALLOCATED,
        RUNNERS_EXCEEDED,
        MEMORY_EXCEEDED
    }
}
