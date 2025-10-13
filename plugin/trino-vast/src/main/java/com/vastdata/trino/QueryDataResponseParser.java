/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.BaseQueryDataResponseParser;
import com.vastdata.client.QueryDataPageBuilder;
import com.vastdata.client.QueryDataPagination;
import com.vastdata.client.VastDebugConfig;
import com.vastdata.client.tx.VastTraceToken;
import io.airlift.log.Logger;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.metrics.Metrics;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;

public class QueryDataResponseParser
        extends BaseQueryDataResponseParser<Page>
{
    private static final Logger LOG = Logger.get(QueryDataResponseParser.class);

    private final QueryDataResponseSchemaConstructor querySchema;

    public QueryDataResponseParser(
            VastTraceToken traceToken, QueryDataResponseSchemaConstructor querySchema,
            VastDebugConfig debugConfig, QueryDataPagination pagination, Optional<Long> limitTotalRows)
    {
        super(traceToken, querySchema.getFields(), pagination, limitTotalRows, debugConfig);
        this.querySchema = querySchema;
        LOG.info("QueryData(%s) QueryDataResponseParser init: schema=%s, requested fields=%s", traceStr, querySchema, fields);
    }

    @Override
    protected QueryDataPageBuilder<Page> createPageBuilder(Schema requestedSchema)
    {
        return new VastPageBuilder(traceStr, requestedSchema);
    }

    @Override
    protected Page joinPages(List<Page> pages)
    {
        verify(!pages.isEmpty());
        int rows = pages.getFirst().getPositionCount();
        int columnCount = pages.stream().mapToInt(Page::getChannelCount).sum();
        Block[] blocks = new Block[columnCount];
        int blockIndex = 0;
        for (Page page : pages) {
            verify(page.getPositionCount() == rows, "QueryData(%s): row count mismatch: %s != %s", traceStr, page.getPositionCount(), rows);
            for (int i = 0; i < page.getChannelCount(); ++i) {
                blocks[blockIndex] = page.getBlock(i);
                blockIndex += 1;
            }
        }
        Page page = querySchema.construct(blocks, rows);
        totalPositions.addAndGet(page.getPositionCount());
        return page;
    }

    public Metrics getMetrics()
    {
        return new Metrics(Map.of(
                "readNanos", new LongCount(readNanos.get()),
                "buildNanos", new LongCount(buildNanos.get()),
                "processNanos", new LongCount(processNanos.get()),
                "totalPositions", new LongCount(totalPositions.get()),
                "totalRequests", new LongCount(1L)));
    }
}
