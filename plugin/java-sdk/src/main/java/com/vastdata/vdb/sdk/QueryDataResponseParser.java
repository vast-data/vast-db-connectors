package com.vastdata.vdb.sdk;

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
        this.allocator = allocator;
        this.schemaHelper = schemaHelper;
    }

    @Override
    protected VectorSchemaRoot joinPages(List<VectorSchemaRoot> list)
    {
        verify(!list.isEmpty());
        int rowCount = list.get(0).getRowCount();
        List<FieldVector> vectors = list
                .stream()
                .flatMap(page -> page.getFieldVectors().stream())
                .collect(Collectors.toList());

        VectorSchemaRoot result = schemaHelper.construct(vectors, rowCount, allocator);
        result.setRowCount(rowCount);
        totalPositions.addAndGet(result.getRowCount());
        LOG.debug("{} joined page: rowCount={}, totalPositions={}", traceStr, rowCount, totalPositions.get());
        return result;
    }

    @Override
    protected QueryDataPageBuilder<VectorSchemaRoot> createPageBuilder(Schema schema)
    {
        return new VastPageBuilder(schema, allocator);
    }
}
