/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.BaseQueryDataResponseParser;
import com.vastdata.client.QueryDataPageBuilder;
import com.vastdata.client.QueryDataPagination;
import com.vastdata.client.VastDebugConfig;
import com.vastdata.client.tx.VastTraceToken;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;

public class QueryDataResponseParser
        extends BaseQueryDataResponseParser<VectorSchemaRoot>
{
    private static final Logger LOG = LoggerFactory.getLogger(QueryDataResponseParser.class);

    private final BufferAllocator allocator;

    public QueryDataResponseParser(
            VastTraceToken traceToken, Schema arrowSchema,
            VastDebugConfig debugConfig, QueryDataPagination pagination, Optional<Long> limitTotalRows, BufferAllocator allocator)
    {
        super(traceToken, arrowSchema.getFields(), pagination, limitTotalRows, debugConfig);
        this.allocator = allocator;
    }

    @Override
    protected QueryDataPageBuilder<VectorSchemaRoot> createPageBuilder(Schema requestedSchema)
    {
        return new VastPageBuilder(requestedSchema, allocator);
    }

    @Override
    protected VectorSchemaRoot joinPages(List<VectorSchemaRoot> pages)
    {
        verify(pages.size() > 0);
        int rowCount = pages.get(0).getRowCount();
        List<FieldVector> vectors = pages
                .stream()
                .flatMap(page -> page.getFieldVectors().stream())
                .collect(Collectors.toList());
        VectorSchemaRoot result = new VectorSchemaRoot(vectors);
        result.setRowCount(rowCount);
        // TODO: handle nested types by merging field vectors correctly
        totalPositions.addAndGet(result.getRowCount());
        LOG.debug("{} joined page: rowCount={}, totalPositions={}", traceStr, rowCount, totalPositions.get());
        return result;
    }

    @Override
    protected void dropPages(List<VectorSchemaRoot> pages)
    {
        pages.forEach(VectorSchemaRoot::close);
    }
}
