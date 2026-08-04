/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

import com.vastdata.client.QueryDataExtraParams;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.bycolumninserter.ByColumnInserter;
import com.vastdata.client.error.VastException;
import com.vastdata.client.metrics.ByColumnInserterMetrics;
import com.vastdata.client.metrics.InsertedRowsStats;
import com.vastdata.client.metrics.RecordBatchSplitterMetrics;
import com.vastdata.client.rowid.RowIDStrategyType;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.spark.VastArrowAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public class VastInsertWrite
        implements VastWriteStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger(
            VastInsertWrite.class);

    private final WriteContext ctx;
    private final ByColumnInserter byColumnInsert;

    public VastInsertWrite(
            Function<VastConfig, VastClient> vastClientSupplier, String dataWriteTraceToken, VastConfig vastConfig, List<URI> dataEndpoints, VastTransaction tx, String schemaName,
            String tableName, String endUser, Set<String> nonUpdatableColumns,
            RowIDStrategyType rowIdType,
            RecordBatchSplitterMetrics splitterMetrics,
            ByColumnInserterMetrics insertMetrics,
            ExecutorService ioExecutor,
            ExecutorService cpuExecutor)
    {
        this.ctx = new WriteContext(vastClientSupplier, dataWriteTraceToken,
                vastConfig, null, tx, schemaName, tableName, endUser);
        InsertedRowsStats insertedRowsStats = new InsertedRowsStats();
        this.byColumnInsert = new ByColumnInserter(
                ctx.getVastClientSupplier().apply(ctx.getVastConfig()),
                ctx.getVastConfig(), rowIdType, ctx.getSchemaName(),
                ctx.getTableName(), ctx.getTx(), dataEndpoints,
                new QueryDataExtraParams(), ctx.getEndUser(), splitterMetrics,
                insertMetrics, nonUpdatableColumns::contains, insertedRowsStats,
                ioExecutor,
                cpuExecutor, ctx.getDataWriteTraceToken());
    }

    @Override
    public void write(VectorSchemaRoot nextChunk)
            throws VastException
    {
        try (nextChunk) {
            int chunkColumnCount = nextChunk.getFieldVectors().size();
            LOG.debug(
                    "{} Inserting next chunk of {} rows, {} columns, hash={}, schema: {}",
                    ctx.getDataWriteTraceToken(), nextChunk.getRowCount(),
                    chunkColumnCount, nextChunk.hashCode(),
                    nextChunk.getSchema());
            try (BufferAllocator inputAllocator = VastArrowAllocator
                    .writeAllocator()
                    .newChildAllocator("InsertWrite", 0, Long.MAX_VALUE)) {
                int chunkRowCount = nextChunk.getRowCount();
                try (VectorSchemaRoot rowIds = byColumnInsert
                        .insert(List.of(nextChunk), inputAllocator)
                        .join()) {
                    int rowIdColumnCount = rowIds.getFieldVectors().size();
                    LOG.debug(
                            "{} Finished inserting chunk of {} rows, received rowIds chunk with {} rows, {} columns and schema: {}",
                            ctx.getDataWriteTraceToken(), chunkRowCount,
                            rowIds.getRowCount(), rowIdColumnCount,
                            rowIds.getSchema());
                }
            }
        }
    }
}
