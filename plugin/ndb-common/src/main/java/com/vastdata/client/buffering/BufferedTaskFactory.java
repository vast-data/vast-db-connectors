/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.buffering;

import com.vastdata.client.metrics.BufferedInsertMetrics;
import org.apache.arrow.memory.BufferAllocator;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface BufferedTaskFactory
{
    CompletableFuture<Void> executeAsync(List<Buffer> buffers,
            BufferedInsertMetrics metrics, BufferAllocator allocator);

    String getTaskName();
}
