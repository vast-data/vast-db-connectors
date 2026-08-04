/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.error.ExceededTotalAllowedBytesPerColumnException;
import com.vastdata.client.metrics.DataResponseParserMetrics;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.memory.MemoryAllocationInfo;
import com.vastdata.memory.MemoryLimiterMetrics;
import com.vastdata.memory.VastMemoryLimiter;
import com.vastdata.trino.metrics.PageSourceMetrics;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.base.metrics.DurationTiming;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.vastdata.client.error.VastExceptionFactory.hasInterruptException;
import static com.vastdata.memory.MemoryAllocationInfo.MemoryAllocationState.ALLOCATED;
import static com.vastdata.memory.MemoryAllocationInfo.MemoryAllocationState.MEMORY_EXCEEDED;

public class VastPageSource
        implements ConnectorPageSource
{
    private static final Logger LOG = Logger.get(VastPageSource.class);

    private final VastTraceToken traceToken;
    private final com.vastdata.memory.VastMemoryLimiter memoryLimiter;
    private final VastSplit split;
    private final Function<Integer, QueryDataResponseParser> fetchPages;
    private final Optional<Long> limitRows;
    private final DataResponseParserMetrics globalDataResponseParserMetrics;
    private final PageSourceMetrics globalVastPageSourceMetrics;
    private final MemoryLimiterMetrics memoryLimiterMetrics;
    private final Metrics.Accumulator parserMetrics;
    private final PageSourceMetrics splitVastPageSourceMetrics;
    private final String allocatorId;
    private final ConnectorSession session;

    private QueryDataResponseParser parser;
    private boolean isFinished;
    private boolean isClosed;

    private int expectedRowsPerPage;
    private boolean isMemoryReserved;
    private long rowReservedMemory;
    private long nextRowReservedMemory;
    private long memoryReserved;
    private long subSplitMemoryReserved;
    private long readRows;
    private long completedBytes;
    private long getNextPageNanos;
    private Long startedNanos;
    private long startDelayNanos = System.nanoTime();
    private long parsedPages;
    private long nullPages;
    private long pageFetchGap;
    private long lastFetchEndTime;
    private int nonAcquiredLimitingAttempts;
    private CompletableFuture<Boolean> permitFuture;

    public VastPageSource(VastTraceToken traceToken,
                          VastMemoryLimiter memoryLimiter,
                          ConnectorSession session,
                          VastSplit split,
                          int expectedRowsPerPage,
                          long estimatedRowsSize,
                          Function<Integer, QueryDataResponseParser> fetchPages,
                          Optional<Long> limitRows,
                          Map<String, Long> additionalMetrics,
                          DataResponseParserMetrics globalDataResponseParserMetrics,
                          PageSourceMetrics vastPageSourceMetrics)
    {
        this.traceToken = traceToken;
        this.memoryLimiter = memoryLimiter;
        this.session = session;
        this.split = split;
        this.fetchPages = fetchPages;
        this.limitRows = limitRows;
        this.parserMetrics = Metrics.accumulator();
        this.expectedRowsPerPage = expectedRowsPerPage;
        this.globalDataResponseParserMetrics = globalDataResponseParserMetrics;
        this.globalVastPageSourceMetrics = vastPageSourceMetrics;
        final Map<String, Metric<?>> m = additionalMetrics.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> new LongCount(e.getValue())));
        parserMetrics.add(new Metrics(m));
        this.splitVastPageSourceMetrics = new PageSourceMetrics();
        splitVastPageSourceMetrics.incActiveSplits();
        this.allocatorId = System.identityHashCode(this) + "-" + traceToken;
        this.rowReservedMemory = estimatedRowsSize;
        this.memoryLimiterMetrics = new MemoryLimiterMetrics();
    }

    private Metrics getPageSourceMetrics()
    {
        Map<String, Metric<?>> metrics = new HashMap<>();
        metrics.putAll(memoryLimiterMetrics.diffMetrics().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new LongCount(e.getValue()))));
        metrics.putAll(splitVastPageSourceMetrics.diffMetrics().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new LongCount(e.getValue()))));
        metrics.putAll(
                Map.of("getNextPageNanos", new LongCount(getNextPageNanos),
                        "pageSourceDurationNanos", new LongCount(startedNanos != null ? System.nanoTime() - startedNanos : 0),
                        "pageSourceStartDelayNanos", new LongCount(startDelayNanos),
                        "pageSourcePageFetchGap", new LongCount(pageFetchGap),
                        "nullPages", new LongCount(nullPages),
                        "parsedPages", new LongCount(parsedPages),
                        "readRows", new LongCount(readRows)));
        return new Metrics(metrics);
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes + (parser != null ? parser.getBytesRead() : 0);
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return isFinished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        long start = System.nanoTime();
        if (parser == null && parsedPages == 0) {
            LOG.debug("QueryData(%s) First page fetch, expectedRowsPerPage=%d, numSubSplits=%d", traceToken, expectedRowsPerPage, split.getContext().getNumOfSubSplits());
            this.startDelayNanos = start - startDelayNanos;
            this.startedNanos = start;
        }
        else {
            this.pageFetchGap += start - lastFetchEndTime;
        }
        try {
            if (isFinished) {
                nullPages += 1;
                LOG.debug("QueryData(%s) Finished page source", traceToken);
                return null;
            }
            int iterationRowsPerPage = expectedRowsPerPage;
            if (parser == null) {
                synchronized (this) {
                    if (isClosed) {
                        LOG.warn("QueryData(%s) avoid allocating when already closed", traceToken);
                        return null;
                    }
                    OptionalInt nextRowsPerPageOpt = tryReserveMemory(iterationRowsPerPage);
                    if (nextRowsPerPageOpt.isEmpty()) {
                        return null;
                    }
                    iterationRowsPerPage = nextRowsPerPageOpt.getAsInt();
                }
                try {
                    parser = fetchPages.apply(iterationRowsPerPage);
                }
                catch (ExceededTotalAllowedBytesPerColumnException e) {
                    expectedRowsPerPage = e.getNumOfRowsSuggestion();
                    memoryLimiter.freeMemory(allocatorId, memoryReserved, VastSessionProperties.isMemoryLimitEnabled(session), memoryLimiterMetrics);
                    isMemoryReserved = false;
                    LOG.debug("QueryData(%s) Exceeded total allowed bytes per column. Returning null page. next row size: %d, currently reserved: %d. err: %s", traceToken, rowReservedMemory, rowReservedMemory, e);
                    nullPages += 1;
                    parser = null;
                    splitVastPageSourceMetrics.incNumExceededMaxBytesPerColumn();
                    return null;
                }
                catch (Exception e) {
                    LOG.debug("QueryData(%s) page fetch had exception %s", traceToken, e);
                    if (hasInterruptException(e)) {
                        Thread.currentThread().interrupt();
                        LOG.warn("QueryData(%s) page fetch was interrupted", traceToken);
                        memoryLimiter.freeMemory(allocatorId, memoryReserved, VastSessionProperties.isMemoryLimitEnabled(session), memoryLimiterMetrics);
                        isMemoryReserved = false;
                    }
                    throw e;
                }
                DataResponseParserMetrics dataResponseParserMetrics = parser.getMetrics();
                Map<String, LongCount> diffMetrics = dataResponseParserMetrics
                        .diffMetrics()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                e -> new LongCount(e.getValue())));
                Map<String, DurationTiming> durationMetrics = dataResponseParserMetrics
                        .durationMetrics()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                e -> new DurationTiming(
                                        new Duration(e.getValue(),
                                                TimeUnit.NANOSECONDS).convertToMostSuccinctTimeUnit())));
                Map<String, Metric<?>> allMetrics = new HashMap<>(diffMetrics);
                allMetrics.putAll(durationMetrics);
                Metrics parserMetric = new Metrics(allMetrics);
                globalDataResponseParserMetrics.merge(dataResponseParserMetrics);
                parserMetrics.add(parserMetric);
                addServerMetrics(parser.getServerMetrics());
            }
            if (parser.hasNext()) {
                SourcePage page = parser.next();
                parsedPages += 1;
                readRows += page.getPositionCount();
                if (page.getPositionCount() > 0) {
                    nextRowReservedMemory = Math.max(page.getRetainedSizeInBytes() / page.getPositionCount(), nextRowReservedMemory);
                }
                memoryLimiter.freeSubSplitMemory(allocatorId, subSplitMemoryReserved, VastSessionProperties.isMemoryLimitEnabled(session), memoryLimiterMetrics);
                memoryReserved -= subSplitMemoryReserved;
                return page;
            }
            if (parser.isSplitFinished()) {
                isFinished = true;
            }
            completedBytes += parser.getBytesRead();
            memoryLimiter.freeMemory(allocatorId, memoryReserved, VastSessionProperties.isMemoryLimitEnabled(session), memoryLimiterMetrics);
            isMemoryReserved = false;
            rowReservedMemory = nextRowReservedMemory != 0 ? nextRowReservedMemory : rowReservedMemory; // next batch of rows memory will be calculated based on the max of all sub-splits
            parser = null;
            LOG.debug("QueryData(%s) Returning null page. next row size: %d, currently reserved: %d", traceToken, rowReservedMemory, rowReservedMemory);
            nullPages += 1;
            return null;
        }
        finally {
            long pageFetchEndTime = System.nanoTime();
            lastFetchEndTime = pageFetchEndTime;
            getNextPageNanos += (pageFetchEndTime - start);
            if (limitRows.map(limit -> readRows >= limit).orElse(false)) {
                if (parser != null) {
                    completedBytes += parser.getBytesRead();
                }
                isFinished = true;
                parser = null;
            }
        }
    }

    /**
     * Attempts to reserve memory for the next page fetch.
     *
     * <p>If memory is already reserved or no memory is required, this method is a no-op and
     * returns the original {@code iterationRowsPerPage} unchanged.
     *
     * <p>If the total requested memory exceeds the global limit ({@code MEMORY_EXCEEDED}), the
     * rows-per-page is halved repeatedly until the request fits, and then the allocation is
     * retried. The returned value reflects the (possibly reduced) rows-per-page that was
     * successfully reserved.
     *
     * <p>If the maximum number of concurrent runners is exceeded ({@code RUNNERS_EXCEEDED}), the
     * caller is blocked via a {@link CompletableFuture} and {@link OptionalInt#empty()} is
     * returned to signal that the caller should yield ({@code return null} from
     * {@link #getNextSourcePage()}).
     *
     * @param iterationRowsPerPage the initially desired number of rows per page
     * @return {@link OptionalInt#empty()} if the split must wait for a permit (RUNNERS_EXCEEDED),
     *         or an {@link OptionalInt} containing the effective rows-per-page to use for the
     *         upcoming {@code fetchPages} call
     */
    private OptionalInt tryReserveMemory(int iterationRowsPerPage)
    {
        long tryAllocateMemory = rowReservedMemory * iterationRowsPerPage * split.getContext().getNumOfSubSplits();
        if (!isMemoryReserved && tryAllocateMemory > 0) {
            MemoryAllocationInfo allocationResponse = memoryLimiter.reserveMemory(allocatorId, tryAllocateMemory,
                    VastSessionProperties.isMemoryLimitEnabled(session),
                    memoryLimiterMetrics);
            while (allocationResponse.getAllocationState() == MEMORY_EXCEEDED) {
                iterationRowsPerPage /= 2;
                tryAllocateMemory = rowReservedMemory * iterationRowsPerPage * split.getContext().getNumOfSubSplits();
                allocationResponse = memoryLimiter.reserveMemory(allocatorId, tryAllocateMemory,
                        VastSessionProperties.isMemoryLimitEnabled(session),
                        memoryLimiterMetrics);
            }
            if (allocationResponse.getAllocationState() == ALLOCATED) {
                memoryReserved = tryAllocateMemory;
                subSplitMemoryReserved = memoryReserved / split.getContext().getNumOfSubSplits();
                if (nonAcquiredLimitingAttempts > 0) {
                    LOG.info("QueryData(%s) limit permit acquired after %d attempts",
                            traceToken,
                            nonAcquiredLimitingAttempts);
                }
                isMemoryReserved = true;
                nonAcquiredLimitingAttempts = 0;
            }
            else if (allocationResponse.getAllocationState() == MemoryAllocationInfo.MemoryAllocationState.RUNNERS_EXCEEDED) {
                this.permitFuture = allocationResponse.getFuture();
                splitVastPageSourceMetrics.incBlockedByMemory();
                nonAcquiredLimitingAttempts++;
                if (nonAcquiredLimitingAttempts % 1000 == 0) {
                    LOG.info("QueryData(%s) waiting for limits permit after %d attempts",
                            traceToken,
                            nonAcquiredLimitingAttempts);
                }
                return OptionalInt.empty();
            }
        }
        return OptionalInt.of(iterationRowsPerPage);
    }

    private void addServerMetrics(Map<String, Long> serverMetrics)
    {
        for (Map.Entry<String, Long> entry : serverMetrics.entrySet()) {
            String metricName = "server_" + entry.getKey();
            Number value = entry.getValue();
            parserMetrics.add(new Metrics(
                    Map.of(metricName, new LongCount(value.longValue()))));
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        synchronized (this) {
            if (isMemoryReserved) {
                memoryLimiter.freeMemory(allocatorId, memoryReserved, VastSessionProperties.isMemoryLimitEnabled(session), memoryLimiterMetrics);
            }
            else if (permitFuture != null && !permitFuture.isDone()) {
                // Split is closed while waiting in RUNNERS_EXCEEDED state.
                // Cancel the future so it is drained harmlessly from waitingFutures
                // instead of wasting a freeMemory signal on a dead split.
                memoryLimiter.cancelWaiting(allocatorId, VastSessionProperties.isMemoryLimitEnabled(session));
            }
            isClosed = true;
        }
        LOG.debug("QueryData(%s) closing %s: %s", traceToken,
                split.getFilters(), getMetrics());
        splitVastPageSourceMetrics.addActiveSplits(-1);
        globalVastPageSourceMetrics.merge(splitVastPageSourceMetrics);
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return permitFuture == null ? NOT_BLOCKED : permitFuture;
    }

    @Override
    public Metrics getMetrics()
    {
        return getPageSourceMetrics().mergeWith(parserMetrics.get());
    }
}
