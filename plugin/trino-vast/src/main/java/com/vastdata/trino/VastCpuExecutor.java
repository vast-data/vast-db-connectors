/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.inject.Inject;
import com.vastdata.client.VastConfig;
import io.airlift.log.Logger;
import jakarta.annotation.PreDestroy;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class VastCpuExecutor
{
    private static final Logger LOG = Logger.get(VastCpuExecutor.class);
    private final ExecutorService executor;

    @Inject
    public VastCpuExecutor(VastConfig config)
    {
        int nThreads = 2 * Runtime.getRuntime().availableProcessors();
        this.executor = Executors.newFixedThreadPool(nThreads);
    }

    public ExecutorService getExecutor()
    {
        return executor;
    }

    @PreDestroy
    public void shutdown()
    {
        LOG.info("Shutting down VastCpuExecutor...");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
            throw new RuntimeException(
                    "Internal Error: io executor should not be interrupted", e);
        }
    }
}
