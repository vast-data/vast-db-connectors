/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.ArrowQueryDataSchemaHelper;
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
import static com.vastdata.spark.SparkArrowVectorUtil.VAST_ROW_ID_TO_SPARK_ROW_ID_VECTOR_ADAPTOR;

public class QueryDataResponseParser
        extends BaseQueryDataResponseParser<VectorSchemaRoot>
{
    private static final Logger LOG = LoggerFactory.getLogger(QueryDataResponseParser.class);

    private final BufferAllocator allocator;
    private final ArrowQueryDataSchemaHelper schemaHelper;

    public QueryDataResponseParser(
            VastTraceToken traceToken, ArrowQueryDataSchemaHelper schemaHelper,
            VastDebugConfig debugConfig, QueryDataPagination pagination, Optional<Long> limitTotalRows, BufferAllocator allocator)
    {
        super(traceToken, schemaHelper.getFields(), pagination, limitTotalRows, debugConfig);
        this.schemaHelper = schemaHelper;
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
        verify(!pages.isEmpty());
        int rowCount = pages.get(0).getRowCount();
        List<FieldVector> vectors = pages
                .stream()
                .flatMap(page -> page.getFieldVectors().stream())
                .map(VAST_ROW_ID_TO_SPARK_ROW_ID_VECTOR_ADAPTOR)
                .collect(Collectors.toList());

        VectorSchemaRoot result = schemaHelper.construct(vectors, rowCount, allocator);
        pages.forEach(VectorSchemaRoot::close);
        vectors.forEach(FieldVector::close);
        totalPositions.addAndGet(rowCount);
        LOG.debug("{} joined page: rowCount={}, totalPositions={}, result: {}", traceStr, result.getRowCount(), totalPositions.get(), result.getSchema());
        return result;
    }

    @Override
    protected void dropPages(List<VectorSchemaRoot> pages)
    {
        pages.forEach(VectorSchemaRoot::close);
    }
}
