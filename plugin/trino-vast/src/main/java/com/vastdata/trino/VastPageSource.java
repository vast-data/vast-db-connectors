/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.tx.VastTraceToken;
import io.airlift.log.Logger;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
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
    private long completedBytes;
    private long getNextPageNanos;
    private final long startedNanos;
    private long parsedPages;
    private long nullPages;
    private final Metrics.Accumulator parserMetrics;

    public VastPageSource(VastTraceToken traceToken, VastSplit split, Supplier<QueryDataResponseParser> fetchPages, Optional<Long> limitRows)
    {
        this.startedNanos = System.nanoTime();
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
                "pageSourceDurationNanos", new LongCount(System.nanoTime() - startedNanos),
                "nullPages", new LongCount(nullPages),
                "parsedPages", new LongCount(parsedPages),
                "readRows", new LongCount(readRows)));
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
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
    public Page getNextPage()
    {
        long start = System.nanoTime();
        try {
            if (isFinished) {
                nullPages += 1;
                LOG.debug("Finished page source");
                return null;
            }
            if (parser == null) {
                parser = fetchPages.get();
                parserMetrics.add(parser.getMetrics());
            }
            if (parser.hasNext()) {
                Page page = parser.next();
                parsedPages += 1;
                completedBytes += page.getSizeInBytes();
                readRows += page.getPositionCount();
                return page;
            }
            if (parser.isSplitFinished()) {
                isFinished = true;
            }
            parser = null;
            LOG.debug("Returning null page");
            nullPages += 1;
            return null;
        }
        finally {
            getNextPageNanos += (System.nanoTime() - start);
            if (limitRows.map(limit -> readRows >= limit).orElse(false)) {
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
        LOG.info("QueryData(%s) closing %s: %s", traceToken, split, getMetrics().getMetrics());
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
