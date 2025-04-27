/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastException;
import com.vastdata.client.executor.RetryStrategy;
import com.vastdata.client.executor.RetryStrategyFactory;
import com.vastdata.client.importdata.ImportDataExecutor;
import com.vastdata.client.importdata.ImportDataFileMapper;
import com.vastdata.client.schema.ImportDataContext;
import com.vastdata.client.schema.ImportDataFile;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.getImportDataHiddenColumnIndex;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.getTablePath;
import static java.lang.String.format;

public class VastBGWriter
        extends ParallelWriteExecutionComponent
{

    private final Logger LOG = LoggerFactory.getLogger(VastBGWriter.class);
    private final Supplier<VectorSchemaRoot> insertChucksSupplier;
    private final String dataWriteTraceToken;
    private final VastTransaction tx;
    private final URI dataEndpoint;
    private final String schemaName;
    private final String tableName;
    private final Function<VastConfig, VastClient> vastClientSupplier;
    private final String modeTraceToken;
    private int processedRows = 0;
    private final VastConfig vastConfig;
    private final VastWriteMode writeMode;

    VastBGWriter(int ordinal,
            Function<VastConfig,VastClient> vastClientSupplier,
            String dataWriteTraceToken,
            VastConfig vastConfig, URI dataEndpoint,
            VastTransaction tx,
            String schemaName,
            String tableName,
            Supplier<VectorSchemaRoot> insertChucksSupplier,
            VastWriteMode writeMode)
    {
        super(ordinal);
        this.vastConfig = vastConfig;
        this.tx = tx;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.dataWriteTraceToken = dataWriteTraceToken;
        this.insertChucksSupplier = insertChucksSupplier;
        this.vastClientSupplier = vastClientSupplier;
        this.dataEndpoint = dataEndpoint;
        this.writeMode = writeMode;
        this.modeTraceToken = format("VastBGWriter[%s]", writeMode);
    }

    @Override
    public void doRun()
    {
        VectorSchemaRoot nextChunk = null;
        VastTraceToken traceToken = tx.generateTraceToken(Optional.of(modeTraceToken));
        try {
            for (nextChunk = pollNext(); nextChunk != null; nextChunk = pollNext()) {
                try {
                    switch (writeMode) {
                        case IMPORT:
                            LOG.debug("{}{} importing data, chuck size: {}, chunk hash: {}", modeTraceToken, dataWriteTraceToken, nextChunk.getRowCount(), nextChunk.hashCode());
                            final int retryMaxCount = this.vastConfig.getRetryMaxCount();
                            final int retrySleepDuration = this.vastConfig.getRetrySleepDuration();
                            boolean parallelImport = this.vastConfig.getParallelImport();
                            Supplier<RetryStrategy> retryStrategy = () -> RetryStrategyFactory.fixedSleepBetweenRetries(retryMaxCount, retrySleepDuration);

                            VectorSchemaRoot finalNextChunk = nextChunk;
                            final int hiddenColumnIndex = getImportDataHiddenColumnIndex(finalNextChunk.getSchema().getFields());
                            List<VectorSchemaRoot> toClose = new ArrayList<>(finalNextChunk.getRowCount());
                            Function<Integer, VectorSchemaRoot> rowSupplier = i -> {
                                LOG.debug("{}{} rowSupplier supplying row no. {} from chunk hash: {}", modeTraceToken, dataWriteTraceToken, i, finalNextChunk.hashCode());
                                VectorSchemaRoot slice = finalNextChunk.slice(i, 1);
                                toClose.add(slice);
                                return slice;
                            };
                            IntFunction<ImportDataFile> importDataFileIntFunction = new ImportDataFileMapper(rowSupplier, hiddenColumnIndex);
                            List<ImportDataFile> sourceFiles = IntStream.range(0, finalNextChunk.getRowCount()).mapToObj(importDataFileIntFunction).collect(Collectors.toList());

                            ImportDataContext ctx = new ImportDataContext(sourceFiles, getTablePath(schemaName, tableName))
                                    .withChunkLimit(vastConfig.getImportChunkLimit());
                            try {
                                new ImportDataExecutor<>(vastClientSupplier.apply(vastConfig)).execute(
                                        ctx, tx, traceToken, Collections.singletonList(dataEndpoint), retryStrategy, parallelImport);
                            }
                            finally {
                                toClose.forEach(VectorSchemaRoot::close);
                            }
                            break;
                        case INSERT:
                            LOG.debug("{}{} Inserting next chunk of {} rows, hash={}, schema: {}", modeTraceToken, dataWriteTraceToken, nextChunk.getRowCount(), nextChunk.hashCode(), nextChunk.getSchema());
                            vastClientSupplier.apply(this.vastConfig).insertRows(tx, schemaName, tableName, nextChunk, dataEndpoint, Optional.empty());
                            break;
                        case DELETE:
                            LOG.debug("{}{} Deleting next chunk of {} rows, hash={}, schema: {}", modeTraceToken, dataWriteTraceToken, nextChunk.getRowCount(), nextChunk.hashCode(), nextChunk.getSchema());
                            vastClientSupplier.apply(this.vastConfig).deleteRows(tx, schemaName, tableName, nextChunk, dataEndpoint, Optional.empty());
                            break;
                        case UPDATE:
                            LOG.debug("{}{} Updating next chunk of {} rows, hash={}, schema: {}", modeTraceToken, dataWriteTraceToken, nextChunk.getRowCount(), nextChunk.hashCode(), nextChunk.getSchema());
                            vastClientSupplier.apply(this.vastConfig).updateRows(tx, schemaName, tableName, nextChunk, dataEndpoint, Optional.empty());
                            break;

                    }
                    int rowCount = nextChunk.getRowCount();
                    nextChunk.close();
                    processedRows += rowCount;
                    LOG.debug("{}{} Processed chunk rows: {}, total processed rows: {}", modeTraceToken, dataWriteTraceToken, rowCount, processedRows);
                }
                catch (VastException e) {
                    throw toRuntime(e);
                }
            }
        }
        finally {
            // release all queued chunks' referenced memory
            if (nextChunk != null) {
                nextChunk.close();
            }
            for (nextChunk = pollNext(); nextChunk != null; nextChunk = pollNext()) {
                nextChunk.close();
            }
        }
        LOG.debug("{}{} was signalled to stop, exiting", modeTraceToken, dataWriteTraceToken);
    }

    @Override
    public String getTaskName()
    {
        return format("%s%s", modeTraceToken, dataWriteTraceToken);
    }

    private VectorSchemaRoot pollNext()
    {
        VectorSchemaRoot vectorSchemaRoot = this.insertChucksSupplier.get();
        if (vectorSchemaRoot != null) {
            LOG.debug("{}{} polled new chunk of {} rows, hash={}", modeTraceToken, dataWriteTraceToken, vectorSchemaRoot.getRowCount(), vectorSchemaRoot.hashCode());
        }
        else {
            LOG.debug("{}{} polled null", modeTraceToken, dataWriteTraceToken);
        }
        return vectorSchemaRoot;
    }
}
