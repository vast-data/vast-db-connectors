/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.tx.VastTraceToken;
import io.airlift.log.Logger;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class VastPageSource
        implements ConnectorPageSource
{
    private static final Logger LOG = Logger.get(VastPageSource.class);

    private final VastTraceToken traceToken;
    private final VastSplit split;
    private final Supplier<QueryDataResponseParser> fetchPages;
    private final Optional<Long> limitRows;

    private QueryDataResponseParser parser;
    private boolean isFinished;

    private long readRows;
    private long completedBytes = 0;
    private long getNextPageNanos;
    private Long startedNanos;
    private long startDelayNanos = System.nanoTime();
    private long parsedPages;
    private long nullPages;
    private long pageFetchGap = 0;
    private long lastFetchEndTime = 0;

    private final Metrics.Accumulator parserMetrics;

    public VastPageSource(VastTraceToken traceToken, VastSplit split, Supplier<QueryDataResponseParser> fetchPages, Optional<Long> limitRows)
    {
        this.traceToken = traceToken;
        this.split = split;
        this.fetchPages = fetchPages;
        this.limitRows = limitRows;
        this.parserMetrics = Metrics.accumulator();
    }

    private Metrics getPageSourceMetrics()
    {
        return new Metrics(Map.of(
                "getNextPageNanos", new LongCount(getNextPageNanos),
                "pageSourceDurationNanos", new LongCount(startedNanos != null ? System.nanoTime() - startedNanos : 0),
                "pageSourceStartDelayNanos", new LongCount(startDelayNanos),
                "pageSourcePageFetchGap", new LongCount(pageFetchGap),
                "nullPages", new LongCount(nullPages),
                "parsedPages", new LongCount(parsedPages),
                "readRows", new LongCount(readRows)));
    }

    @Override
    public long getCompletedBytes()
    {
        LOG.debug("completed bytes: %d",completedBytes + (parser != null? parser.getBytesRead() : 0));
        return completedBytes + (parser != null? parser.getBytesRead() : 0);
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
    public SourcePage getNextSourcePage() {
        long start = System.nanoTime();
        if (parser == null && parsedPages == 0) {
            LOG.debug("QueryData(%s) First page fetch", traceToken);
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
            if (parser == null) {
                parser = fetchPages.get();
                parserMetrics.add(parser.getMetrics());
            }
            if (parser.hasNext()) {
                SourcePage page = parser.next();
                parsedPages += 1;
                readRows += page.getPositionCount();
                return page;
            }
            if (parser.isSplitFinished()) {
                isFinished = true;
            }
            completedBytes += parser.getBytesRead();
            parser = null;
            LOG.debug("QueryData(%s) Returning null page", traceToken);
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

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        LOG.debug("QueryData(%s) closing %s: %s", traceToken, split, getMetrics().getMetrics());
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.empty();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public Metrics getMetrics()
    {
        return getPageSourceMetrics().mergeWith(parserMetrics.get());
    }
}
