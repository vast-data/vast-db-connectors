/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.componenttests;

import com.vastdata.ShapingLoggerFactory;
import com.vastdata.client.ArrowQueryDataSchemaHelper;
import com.vastdata.client.BaseQueryDataResponseParser;
import com.vastdata.client.QueryDataPageBuilder;
import com.vastdata.client.QueryDataPagination;
import com.vastdata.client.VastDebugConfig;
import com.vastdata.client.tx.VastTraceToken;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;

public class QueryDataResponseParser
        extends BaseQueryDataResponseParser<VectorSchemaRoot, Void>
{
    private final RootAllocator allocator;
    private final ArrowQueryDataSchemaHelper schemaHelper;

    public QueryDataResponseParser(ShapingLoggerFactory shapingLoggerFactory,
                                   VastTraceToken traceToken,
                                   ArrowQueryDataSchemaHelper schemaHelper,
                                   VastDebugConfig debugConfig,
                                   QueryDataPagination pagination,
                                   Optional<Long> limitTotalRows,
                                   RootAllocator allocator,
                                   Optional<Long> limitTotalBytes)
    {
        super(shapingLoggerFactory, traceToken, schemaHelper.getFields(),
                pagination, limitTotalRows, debugConfig, limitTotalBytes);
        this.allocator = allocator;
        this.schemaHelper = schemaHelper;
    }

    @Override
    protected VectorSchemaRoot joinPages(List<VectorSchemaRoot> list,
            QueryDataPageBuilder<VectorSchemaRoot, Void> pageBuilder)
    {
        verify(!list.isEmpty());
        int rowCount = list.get(0).getRowCount();
        List<FieldVector> vectors = list
                .stream()
                .flatMap(page -> page.getFieldVectors().stream())
                .collect(Collectors.toList());

        VectorSchemaRoot result = schemaHelper.construct(vectors, rowCount,
                allocator);
        result.setRowCount(rowCount);
        metrics.addTotalPositions(result.getRowCount());
        return result;
    }

    @Override
    protected QueryDataPageBuilder<VectorSchemaRoot, Void> createPageBuilder(
            Schema schema)
    {
        return new VastPageBuilder(schema, allocator);
    }
}
