/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.memory;

import com.google.inject.Inject;
import com.vastdata.client.VastConfig;
import io.airlift.log.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newScheduledThreadPool;

public class VastMemoryLimiter
{
    private static final Logger LOG = Logger.get(VastMemoryLimiter.class);

    private final long memoryLimit;
    private final int maxRunnerFactor;
    final Queue<CompletableFuture<Boolean>> waitingFutures = new ConcurrentLinkedQueue<>();
    final Map<String, MemoryAllocationInfo> waitingAllocations = new ConcurrentHashMap<>(); // used for releasing hanging futures.
    final Set<String> runningAllocatedSplits = ConcurrentHashMap.newKeySet();
    private final Duration releasePeriod;
    private final MemoryLimiterMetrics globalMemoryLimiterMetrics;

    @Inject
    public VastMemoryLimiter(VastConfig vastConfig, MemoryLimiterMetrics globalMemoryLimiterMetrics)
    {
        this.memoryLimit = vastConfig.getMemoryLimiterMaxAllowed().toBytes();
        this.maxRunnerFactor = vastConfig.getMemoryLimitMaxNumRunnerFactor();
        this.releasePeriod = vastConfig.getMemoryLimiterHangingReleasePeriod();
        this.globalMemoryLimiterMetrics = globalMemoryLimiterMetrics;
        ScheduledExecutorService executorService = newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(this::releaseHangingFutures, 0, vastConfig.getMemoryLimiterHangingValidationInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    private void releaseHangingFutures()
    {
        long currentTime = System.currentTimeMillis();
        for (Map.Entry<String, MemoryAllocationInfo> entry : waitingAllocations.entrySet()) {
            String allocatorId = entry.getKey();
            MemoryAllocationInfo allocationInfo = entry.getValue();
            if (currentTime - allocationInfo.getTimestamp() > releasePeriod.toMillis()) {
                LOG.warn("Releasing hanging future for allocatorId(%s) that has been waiting for %d minutes." +
                                " This may indicate a memory leak or a long-running query. allocationInfo (timestamp: %d, amount: %d))",
                        allocatorId, TimeUnit.MILLISECONDS.toMinutes(currentTime - allocationInfo.getTimestamp()), allocationInfo.getTimestamp(), allocationInfo.getMemoryRequestAmount());
                allocationInfo.getFuture().complete(true);
            }
        }
    }

    /**
     * Attempts to reserve {@code memoryRequestAmount} bytes for the given allocator.
     *
     * <p>Three outcomes are possible, reflected in {@link MemoryAllocationInfo#getAllocationState()}:
     * <ul>
     *   <li>{@link MemoryAllocationInfo.MemoryAllocationState#ALLOCATED} – memory was successfully reserved and the
     *       caller may proceed immediately. {@link MemoryAllocationInfo#getFuture()} is {@code null}.</li>
     *   <li>{@link MemoryAllocationInfo.MemoryAllocationState#RUNNERS_EXCEEDED} – too many concurrent allocators are
     *       already running. The caller should block on {@link MemoryAllocationInfo#getFuture()}, which
     *       will be completed by a subsequent {@link #freeMemory} call or by the background
     *       hanging-future release task.</li>
     *   <li>{@link MemoryAllocationInfo.MemoryAllocationState#MEMORY_EXCEEDED} – the requested amount would exceed the
     *       configured memory limit (or the predictive slot estimate). The caller should reduce its
     *       request size or back off. {@link MemoryAllocationInfo#getFuture()} is {@code null}.</li>
     * </ul>
     *
     * <p>When {@code isMemoryLimitEnabled} is {@code false} the method always returns
     * {@link MemoryAllocationInfo.MemoryAllocationState#ALLOCATED} without touching any counters.
     *
     * @param allocatorId           unique identifier for the caller (used for logging and duplicate detection)
     * @param memoryRequestAmount   number of bytes to reserve
     * @param isMemoryLimitEnabled  whether the memory-limiting logic should be applied
     * @param memoryLimiterMetrics  per-caller metrics object updated on each code path
     * @return an {@link MemoryAllocationInfo} describing the outcome of the reservation attempt
     */
    public synchronized MemoryAllocationInfo reserveMemory(String allocatorId, long memoryRequestAmount, boolean isMemoryLimitEnabled, MemoryLimiterMetrics memoryLimiterMetrics)
    {
        if (!isMemoryLimitEnabled) {
            return new MemoryAllocationInfo(
                    MemoryAllocationInfo.MemoryAllocationState.ALLOCATED, memoryRequestAmount, null);
        }
        if (globalMemoryLimiterMetrics.getMemoryAllocated() + memoryRequestAmount > memoryLimit) {
            LOG.warn("AllocatorId(%s) Reserving memory request amount exceeded for memory limit. requesting: %d, current: %d", allocatorId, memoryRequestAmount, globalMemoryLimiterMetrics.getMemoryAllocated());
            memoryLimiterMetrics.incMemoryExceeded();
            return new MemoryAllocationInfo(
                    MemoryAllocationInfo.MemoryAllocationState.MEMORY_EXCEEDED, memoryRequestAmount, null);
        }
        else if (!runningAllocatedSplits.contains(allocatorId) && runningAllocatedSplits.size() > Runtime.getRuntime().availableProcessors() * maxRunnerFactor) {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            LOG.debug("AllocatorId(%s) too many running threads. current: %d", allocatorId, globalMemoryLimiterMetrics.getMemoryAllocated());
            waitingFutures.add(future);
            MemoryAllocationInfo response = new MemoryAllocationInfo(
                    MemoryAllocationInfo.MemoryAllocationState.RUNNERS_EXCEEDED, memoryRequestAmount, future);
            waitingAllocations.put(allocatorId, response);
            return response;
        }
        else {
            long availableMemory = memoryLimit - memoryLimiterMetrics.getMemoryAllocated();
            int remainingSlots = (Runtime.getRuntime().availableProcessors() * 2) - runningAllocatedSplits.size();
            if (remainingSlots * memoryRequestAmount > availableMemory) {
                LOG.warn("AllocatorId(%s) memory prediction estimate . requesting: %d, current: %d", allocatorId, memoryRequestAmount, globalMemoryLimiterMetrics.getMemoryAllocated());
                memoryLimiterMetrics.incMemoryExceeded();
                return new MemoryAllocationInfo(
                        MemoryAllocationInfo.MemoryAllocationState.MEMORY_EXCEEDED, memoryRequestAmount, null);
            }
            globalMemoryLimiterMetrics.addMemoryAllocated(memoryRequestAmount);
            globalMemoryLimiterMetrics.incMemoryAcquired();
            memoryLimiterMetrics.incMemoryAcquired();
            if (memoryRequestAmount > 0) {
                LOG.debug("AllocatorId(%s) Reserving memory request amount success. requesting: %d, current: %d", allocatorId, memoryRequestAmount, globalMemoryLimiterMetrics.getMemoryAllocated());
            }
            runningAllocatedSplits.add(allocatorId);
            globalMemoryLimiterMetrics.incRunningAllocatedSplits();
            return new MemoryAllocationInfo(
                    MemoryAllocationInfo.MemoryAllocationState.ALLOCATED, memoryRequestAmount, null);
        }
    }

    public synchronized void cancelWaiting(String allocatorId, boolean isMemoryLimitEnabled)
    {
        if (!isMemoryLimitEnabled) {
            return;
        }
        MemoryAllocationInfo allocationInfo = waitingAllocations.remove(allocatorId);
        if (allocationInfo != null && allocationInfo.getFuture() != null) {
            LOG.info("AllocatorId(%s) cancelling waiting future due to close", allocatorId);
            allocationInfo.getFuture().cancel(false);
        }
    }

    public synchronized void freeSubSplitMemory(String allocatorId, long memoryFreeAmount, boolean isMemoryLimitEnabled, MemoryLimiterMetrics memoryLimiterMetrics)
    {
        if (!isMemoryLimitEnabled) {
            return;
        }
        MemoryAllocationInfo allocationInfo = waitingAllocations.get(allocatorId);
        globalMemoryLimiterMetrics.addMemoryAllocated(-1 * memoryFreeAmount);
        triggerWaitingFuture(allocatorId, allocationInfo);
    }

    public synchronized void freeMemory(String allocatorId, long memoryFreeAmount, boolean isMemoryLimitEnabled, MemoryLimiterMetrics memoryLimiterMetrics)
    {
        if (!isMemoryLimitEnabled) {
            return;
        }
        runningAllocatedSplits.remove(allocatorId);
        globalMemoryLimiterMetrics.addRunningAllocatedSplits(-1);
        globalMemoryLimiterMetrics.addMemoryAllocated(-1 * memoryFreeAmount);
        if (memoryFreeAmount > 0) {
            LOG.debug("AllocatorId(%s) free memory success. freeing: %d, current: %d", allocatorId, memoryFreeAmount, globalMemoryLimiterMetrics.getMemoryAllocated());
        }
        MemoryAllocationInfo allocationInfo = waitingAllocations.remove(allocatorId);
        if (allocationInfo != null) {
            LOG.debug("AllocatorId(%s) has a waiting future. Completing it now. allocationInfo running for: %d",
                    allocatorId, System.currentTimeMillis() - allocationInfo.getTimestamp());
            if (allocationInfo.getMemoryRequestAmount() != memoryFreeAmount) {
                LOG.warn(
                        "AllocatorId(%s) memory free amount is different than allocationInfo amount. freeing: %d, requesting: %d",
                        allocatorId, memoryFreeAmount, allocationInfo.getMemoryRequestAmount());
            }
        }

        memoryLimiterMetrics.incMemoryReleased();
        globalMemoryLimiterMetrics.incMemoryReleased();
        triggerWaitingFuture(allocatorId, allocationInfo);
    }

    private void triggerWaitingFuture(String allocatorId,
                                      MemoryAllocationInfo allocationInfo)
    {
        CompletableFuture<Boolean> waitingFuture = waitingFutures.poll();
        boolean wasCompletedNow = false;
        while (!wasCompletedNow && waitingFuture != null) {
            wasCompletedNow = waitingFuture.complete(true);
            if (!wasCompletedNow) {
                waitingFuture = waitingFutures.poll();
                if (allocationInfo != null) {
                    LOG.warn("AllocatorId(%s) has a waiting future but it's already completed. allocationInfo timestamp: %d",
                            allocatorId, allocationInfo.getTimestamp());
                }
            }
        }
    }
}
