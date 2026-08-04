/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.collect.ImmutableList;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.buffering.BufferedDml;
import com.vastdata.client.buffering.BufferedTaskFactory;
import com.vastdata.client.buffering.VsrAppender;
import com.vastdata.client.buffering.delete.BufferedDeleteTaskFactory;
import com.vastdata.client.buffering.insert.BufferedInsertTaskFactory;
import com.vastdata.client.buffering.update.BufferedUpdateTaskFactory;
import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.client.metrics.BufferedInsertMetrics;
import com.vastdata.client.metrics.ByColumnInserterMetrics;
import com.vastdata.client.metrics.InsertedRowsStats;
import com.vastdata.client.metrics.RecordBatchSplitterMetrics;
import com.vastdata.trino.block.BlockApiFactory;
import com.vastdata.trino.block.Int128ArrayBlockApi;
import com.vastdata.trino.partition.PartitionKeyHashFunction;
import com.vastdata.trino.rowid.TrinoRowIDFieldFactory;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeOperators;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.vastdata.client.error.VastExceptionFactory.hasInterruptException;
import static com.vastdata.client.schema.ArrowSchemaUtils.VASTDB_ROW_ID_FIELD;
import static com.vastdata.trino.VastMergePage.createVastUpdateDeleteInsertPages;
import static io.trino.spi.StandardErrorCode.READ_ONLY_VIOLATION;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class VastMergeSink
        implements ConnectorMergeSink
{
    private static final long NON_SORTED_PARTITION_ID = 1L;
    private static final Logger LOG = Logger.get(VastMergeSink.class);
    private final VastTrinoExceptionFactory vastTrinoExceptionFactory = new VastTrinoExceptionFactory();
    private final VastMergeTableHandle mergeHandle;
    private final ConnectorSession session;
    private final List<URI> dataEndpoints;
    private final BufferedDml bufferedDeleter;
    private final BufferedDml bufferedInserter;
    private final BufferedDml bufferedUpdater;
    private final BiFunction<Page, Integer, Long> insertRowBufferAssigner;

    private final BufferAllocator allocator;
    private final BufferAllocator insertAllocator;
    private final BufferAllocator deleteAllocator;
    private final BufferAllocator updateAllocator;
    private final String traceToken;

    public VastMergeSink(VastClient client,
                         VastConfig config,
                         ConnectorSession session,
                         VastTransactionHandle transactionHandle,
                         VastMergeTableHandle mergeHandle,
                         ConnectorPageSinkId pageSinkId,
                         BufferAllocator insertBuffersAllocator,
                         BufferedInsertMetrics globalBufferedInsertMetrics,
                         List<URI> dataEndpoints,
                         VastIoExecutor ioExecutor,
                         VastCpuExecutor cpuExecutor,
                         TypeOperators typeOperators)
    {
        this.mergeHandle = mergeHandle;
        this.session = session;
        this.dataEndpoints = dataEndpoints;
        this.allocator = insertBuffersAllocator.newChildAllocator(
                String.format("MergeSink-%d", pageSinkId.getId()), 0,
                Long.MAX_VALUE);
        this.insertAllocator = allocator.newChildAllocator("insert", 0,
                Long.MAX_VALUE);
        this.updateAllocator = allocator.newChildAllocator("update", 0,
                Long.MAX_VALUE);
        this.deleteAllocator = allocator.newChildAllocator("delete", 0,
                Long.MAX_VALUE);

        final BufferedTaskFactory updateFactory = new BufferedUpdateTaskFactory(
                client, transactionHandle,
                mergeHandle.getTable().getSchemaName(),
                mergeHandle.getTable().getTableName(), mergeHandle.getTable().getExtraQueryParams(), session.getUser(),
                dataEndpoints, ioExecutor.getExecutor());
        final BufferedTaskFactory deleteFactory = new BufferedDeleteTaskFactory(
                client, transactionHandle,
                mergeHandle.getTable().getSchemaName(),
                mergeHandle.getTable().getTableName(), mergeHandle.getTable().getExtraQueryParams(), session.getUser(),
                dataEndpoints, ioExecutor.getExecutor());

        final VastTableHandle insertTable = mergeHandle.getTable();
        this.traceToken = transactionHandle.generateTraceToken(
                session.getTraceToken()).toString();
        BufferedTaskFactory insertFactory = new BufferedInsertTaskFactory(
                client, config, new RecordBatchSplitterMetrics(),
                new ByColumnInserterMetrics(),
                insertTable.getRowIdStrategyType(), insertTable.getSchemaName(),
                insertTable.getTableName(), transactionHandle, dataEndpoints,
                mergeHandle.getTable().getExtraQueryParams(), session.getUser(),
                insertTable.getIsNonUpdateableColumnPredicate(),
                new InsertedRowsStats(), ioExecutor.getExecutor(),
                cpuExecutor.getExecutor(), this.traceToken);

        BufferedDml.Config bufferedDmlConfig = new BufferedDml.Config(
                VastSessionProperties.getInsertBuffersOpenBufferRowCount(
                        session),
                VastSessionProperties.getMaxRequestBodySize(session),
                VastSessionProperties.getInsertBufferOpenVsrCountPreallocation(
                        session),
                VastSessionProperties.getInsertBuffersTargetNodeMaxBufferSize(
                        session),
                VastSessionProperties.getInsertBufferTargetRowCountPerPartitionFlush(
                        session), config.getBufferedInserterMaxWritePermits(),
                config.getBufferedInserterMaxJobPermits());
        this.bufferedDeleter = new BufferedDml(bufferedDmlConfig,
                deleteAllocator, insertBuffersAllocator,
                globalBufferedInsertMetrics, deleteFactory,
                new InsertedRowsStats(), ioExecutor.getExecutor(),
                cpuExecutor.getExecutor());
        this.bufferedInserter = new BufferedDml(bufferedDmlConfig,
                insertAllocator, insertBuffersAllocator,
                globalBufferedInsertMetrics, insertFactory,
                new InsertedRowsStats(), ioExecutor.getExecutor(),
                cpuExecutor.getExecutor());
        this.bufferedUpdater = new BufferedDml(bufferedDmlConfig,
                updateAllocator, insertBuffersAllocator,
                globalBufferedInsertMetrics, updateFactory,
                new InsertedRowsStats(), ioExecutor.getExecutor(),
                cpuExecutor.getExecutor());

        VastTableHandle table = this.mergeHandle.getTable();
        table.setColumnHandlesCache(
                this.mergeHandle.getTable().getMergedColumns());
        this.insertRowBufferAssigner = table
                .getPartitioningHandle()
                .map(p -> (BiFunction<Page, Integer, Long>) PartitionKeyHashFunction.create(
                        p.partitionFunctions(), typeOperators,
                        PartitionKeyHashFunction.IndexBase.BY_COLUMN_INDEX))
                .orElse((_, _) -> (long) 0);

        LOG.debug("[%d] VastMergeSink initialized",
                System.identityHashCode(this));
    }

    @Override
    public void storeMergedRows(Page origPage)
    {
        final String endUser = session.getUser();
        int columnCount = mergeHandle.getTable().getMergedColumns().size();
        LOG.debug(
                "storeMergeRows: mergeableColumns: %s, page column count: %s, endUser=%s",
                mergeHandle.getColumns(), origPage.getChannelCount(), endUser);
        VastMergePage vastMergePage = createVastUpdateDeleteInsertPages(
                origPage, columnCount);

        vastMergePage.getDeletePage().ifPresent(page ->
        {
            Map<Long, VsrAppender> partitionToRowMap = splitUpdateDeletePageToPartitions(
                    page, Collections.emptySet(), deleteAllocator);
            writePage(partitionToRowMap, bufferedDeleter);
        });
        vastMergePage.getUpdatePage().ifPresent(page ->
        {
            Set<String> updateColumnNames = mergeHandle
                    .getColumns()
                    .stream()
                    .map(col -> col.getField().getName())
                    .collect(Collectors.toSet());
            Map<Long, VsrAppender> partitionToRowMap = splitUpdateDeletePageToPartitions(
                    page, updateColumnNames, updateAllocator);
            writePage(partitionToRowMap, bufferedUpdater);
        });
        vastMergePage.getInsertPage().ifPresent(page ->
        {
            Schema insertSchema = new Schema(mergeHandle
                    .getTable()
                    .getMergedColumns()
                    .stream()
                    .map(VastColumnHandle::getField)
                    .collect(Collectors.toList()));
            Map<Long, VsrAppender> partitionToRowMap = PageToVsr.getPageToBufferVsrAppender(
                    page,
                    insertRowBufferAssigner,
                    insertAllocator,
                    insertSchema);

            writePage(partitionToRowMap, bufferedInserter);
        });
    }

    private void writePage(Map<Long, VsrAppender> partitionToRowMap,
                           BufferedDml bufferedDml)
    {
        try {
            bufferedDml.write(partitionToRowMap);
        }
        catch (VastRuntimeException e) {
            for (VsrAppender vsrAppender : partitionToRowMap.values()) {
                try {
                    vsrAppender.close();
                }
                catch (Throwable closeEx) {
                    e.addSuppressed(closeEx);
                }
            }
            if (hasInterruptException(e)) {
                Thread.currentThread().interrupt();
            }
            throw vastTrinoExceptionFactory.fromVastRuntimeException(e);
        }
    }

    /**
     * Efficiently maps partition IDs to an array of primitive positions,
     * avoiding Integer boxing.
     */
    private Map<Long, int[]> getPartitionsToPositions(Block block)
    {
        int positionCount = block.getPositionCount();
        boolean is128Block = block instanceof Int128ArrayBlock || (block instanceof DictionaryBlock && ((DictionaryBlock) block).getDictionary() instanceof Int128ArrayBlock) || (block instanceof RunLengthEncodedBlock && ((RunLengthEncodedBlock) block).getValue() instanceof Int128ArrayBlock);

        if (!is128Block) {
            int[] allPositions = new int[positionCount];
            for (int i = 0; i < positionCount; i++) {
                allPositions[i] = i;
            }
            return Map.of(NON_SORTED_PARTITION_ID, allPositions);
        }

        Int128ArrayBlockApi int128ArrayBlockApi = BlockApiFactory.getInt128ApiInstance(
                block);

        // Pass 1: Count positions per partition to pre-size arrays
        Map<Long, Integer> partitionCounts = new HashMap<>();
        for (int pos = 0; pos < positionCount; pos++) {
            long partitionId = int128ArrayBlockApi.getInt128High(pos);
            partitionCounts.merge(partitionId, 1, Integer::sum);
        }

        // Pass 2: Allocate primitive arrays and populate them
        Map<Long, int[]> partitionToPositions = new HashMap<>();
        Map<Long, Integer> partitionIndexes = new HashMap<>();
        for (Map.Entry<Long, Integer> entry : partitionCounts.entrySet()) {
            partitionToPositions.put(entry.getKey(), new int[entry.getValue()]);
            partitionIndexes.put(entry.getKey(), 0);
        }

        for (int pos = 0; pos < positionCount; pos++) {
            long partitionId = int128ArrayBlockApi.getInt128High(pos);
            int[] positions = partitionToPositions.get(partitionId);
            int idx = partitionIndexes.get(partitionId);
            positions[idx] = pos;
            partitionIndexes.put(partitionId, idx + 1);
        }

        return partitionToPositions;
    }

    /**
     * Splits a whole Page into partitioned Pages using Trino's zero-copy
     * getPositions. Treats the first block (index 0) as the rowId block.
     */
    private Map<Long, Page> splitPartitionsToPage(Page page)
    {
        Block rowIdBlock = page.getBlock(0);
        Map<Long, int[]> partitionToPositions = getPartitionsToPositions(
                rowIdBlock);

        Map<Long, Page> partitionToPage = new HashMap<>();
        for (Map.Entry<Long, int[]> entry : partitionToPositions.entrySet()) {
            int[] positions = entry.getValue();
            partitionToPage.put(entry.getKey(),
                    page.getPositions(positions, 0, positions.length));
        }

        return partitionToPage;
    }

    private Map<Long, VsrAppender> splitUpdateDeletePageToPartitions(Page page,
                                                                     Set<String> updateColumnNames,
                                                                     BufferAllocator allocator)
    {
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        List<Block> filteredBlocks = new ArrayList<>();

        // row_id block
        fields.add(
                TrinoRowIDFieldFactory.INSTANCE.apply(mergeHandle.getTable()));
        filteredBlocks.add(page.getBlock(0));

        for (int i = 0; i < mergeHandle
                .getTable()
                .getMergedColumns()
                .size(); i++) {
            VastColumnHandle col = mergeHandle
                    .getTable()
                    .getMergedColumns()
                    .get(i);
            if (updateColumnNames.contains(col.getField().getName())) {
                fields.add(col.getField());
                filteredBlocks.add(
                        page.getBlock(i + 1)); // +1 because block 0 is row_id
            }
        }
        VastRecordBatchBuilder builder = new VastRecordBatchBuilder(
                new Schema(fields.build()), allocator);

        Page filteredPage = new Page(page.getPositionCount(),
                filteredBlocks.toArray(new Block[0]));

        // 2. Split into partitions and build VSRs directly from the filtered page
        Map<Long, VsrAppender> partitionToRowMap = new HashMap<>();
        Map<Long, Page> partitionToPage = splitPartitionsToPage(filteredPage);

        for (Map.Entry<Long, Page> entry : partitionToPage.entrySet()) {
            VectorSchemaRoot vsr = builder.build(entry.getValue());
            validateRowIdColumns(mergeHandle, vsr);
            partitionToRowMap.put(entry.getKey(),
                    new VsrMergeAppender(entry.getKey(), vsr));
        }

        LOG.debug("[%d] partitionToRowMap: %s", System.identityHashCode(this),
                partitionToRowMap.keySet());
        return partitionToRowMap;
    }

    private void validateRowIdColumns(VastMergeTableHandle mergeHandle,
                                      VectorSchemaRoot root)
    {
        FieldVector rowIdVector = root.getVector(0);

        verify(rowIdVector.getNullCount() == 0, "%s cannot contain NULLs",
                rowIdVector.getName());

        FieldVector vastDbRowIdVector = root.getVector(
                VASTDB_ROW_ID_FIELD.getName());
        if (vastDbRowIdVector != null) {
            ArrowBuf buf1 = rowIdVector.getDataBuffer();
            ArrowBuf buf2 = vastDbRowIdVector.getDataBuffer();
            if (ByteFunctionHelpers.compare(buf1, 0, buf1.readableBytes(), buf2,
                    0, buf2.readableBytes()) != 0) {
                // Workaround for ORION-158361 (drop `vastdb_rowid` column when doing updates)
                String msg = String.format(
                        "Cannot modify %s, table=%s batch=%s",
                        VASTDB_ROW_ID_FIELD.getName(), mergeHandle.getTable(),
                        root.contentToTSVString());
                LOG.error(msg);
                throw new TrinoException(READ_ONLY_VIOLATION, msg);
            }
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        close();
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        LOG.warn("[%d] aborting merge sink", System.identityHashCode(this));
        close();
    }

    private void close()
    {
        try {
            bufferedDeleter.flushAllAndFinish();
            deleteAllocator.close();

            bufferedInserter.flushAllAndFinish();
            insertAllocator.close();

            bufferedUpdater.flushAllAndFinish();
            updateAllocator.close();

            allocator.close();
        }
        catch (VastRuntimeException e) {
            throw vastTrinoExceptionFactory.fromVastRuntimeException(e);
        }
        catch (Exception e) {
            throw vastTrinoExceptionFactory.fromThrowable(e);
        }
    }

    public List<URI> getShuffledDataEndpoints()
    {
        return this.dataEndpoints;
    }

    private static class VsrMergeAppender
            implements VsrAppender
    {
        private final long id;
        private final VectorSchemaRoot vsr;

        public VsrMergeAppender(long id, VectorSchemaRoot vsr)
        {
            this.id = id;
            this.vsr = requireNonNull(vsr);
        }

        public Integer getRowCount()
        {
            return this.vsr.getRowCount();
        }

        public void append(VectorSchemaRoot root)
        {
            VectorSchemaRootAppender.append(false, root, vsr);
            LOG.debug("[%s] appending %d rows. now have %d", id,
                    vsr.getRowCount(), root.getRowCount());
        }

        @Override
        public Schema getSchema()
        {
            return vsr.getSchema();
        }

        public void close()
        {
            LOG.debug("[%s] closing VSR with %d rows", id, vsr.getRowCount());
            vsr.close();
        }
    }
}
