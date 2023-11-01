/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

import com.google.common.annotations.VisibleForTesting;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.executor.RetryStrategy;
import com.vastdata.client.executor.RetryStrategyFactory;
import com.vastdata.client.importdata.ImportDataExecutor;
import com.vastdata.client.importdata.ImportDataFileMapper;
import com.vastdata.client.schema.ImportDataContext;
import com.vastdata.client.schema.ImportDataFile;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
import ndb.NDB;
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

public class BGInserter
        extends ParallelWriteExecutionComponent
{
    public static final Function<VastConfig, VastClient> VAST_CLIENT_SUPPLIER_FROM_SPARK_CONTEXT = vastConfig -> {
        try {
            return NDB.getVastClient(vastConfig);
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
    };

    private final Logger LOG = LoggerFactory.getLogger(BGInserter.class);
    private final Supplier<VectorSchemaRoot> insertChucksSupplier;
    private final String dataWriteTraceToken;
    private final VastTransaction tx;
    private final URI dataEndpoint;
    private final String schemaName;
    private final String tableName;
    private final boolean forImportData;
    private final Function<VastConfig, VastClient> vastClientSupplier;
    private int insertedRowsCount = 0;
    private final VastConfig vastConfig;

    public BGInserter(int ordinal, String dataWriteTraceToken, VastConfig vastConfig, URI dataEndpoint,
            VastTransaction tx, String schemaName, String tableName,
            Supplier<VectorSchemaRoot> insertChucksSupplier, boolean forImportData)
    {
        this(ordinal, VAST_CLIENT_SUPPLIER_FROM_SPARK_CONTEXT, dataWriteTraceToken,
                vastConfig, dataEndpoint, tx, schemaName, tableName, insertChucksSupplier, forImportData);
    }

    @VisibleForTesting
    protected BGInserter(
            int ordinal,
            Function<VastConfig,VastClient> vastClientSupplier,
            String dataWriteTraceToken,
            VastConfig vastConfig, URI dataEndpoint,
            VastTransaction tx,
            String schemaName,
            String tableName,
            Supplier<VectorSchemaRoot> insertChucksSupplier,
            boolean forImportData)
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
        this.forImportData = forImportData;
    }

    @Override
    public void doRun()
    {
        VectorSchemaRoot nextChunk = null;
        VastTraceToken traceToken = tx.generateTraceToken(Optional.of("BGInserter"));
        try {
            for (nextChunk = pollNext(); nextChunk != null; nextChunk = pollNext()) {
                try {
                    if (forImportData) {
                        LOG.debug("BGInserter{} importing data", dataWriteTraceToken);
                        final int retryMaxCount = this.vastConfig.getRetryMaxCount();
                        final int retrySleepDuration = this.vastConfig.getRetrySleepDuration();
                        boolean parallelImport = this.vastConfig.getParallelImport();
                        Supplier<RetryStrategy> retryStrategy = () -> RetryStrategyFactory.fixedSleepBetweenRetries(retryMaxCount, retrySleepDuration);

                        VectorSchemaRoot finalNextChunk = nextChunk;
                        final int hiddenColumnIndex = getImportDataHiddenColumnIndex(finalNextChunk.getSchema().getFields());
                        List<VectorSchemaRoot> toClose = new ArrayList<>(finalNextChunk.getRowCount());
                        Function<Integer, VectorSchemaRoot> rowSupplier = i -> {
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
                    }
                    else {
                        LOG.debug("BGInserter{} Inserting next chunk of {} rows, hash={}", dataWriteTraceToken, nextChunk.getRowCount(), nextChunk.hashCode());
                        vastClientSupplier.apply(this.vastConfig).insertRows(tx, schemaName, tableName, nextChunk, dataEndpoint, Optional.empty());
                    }
                    int rowCount = nextChunk.getRowCount();
                    nextChunk.close();
                    insertedRowsCount += rowCount;
                    LOG.debug("BGInserter{} {} chunk rows: {}, total inserted rows: {}", dataWriteTraceToken, forImportData ? "Imported" : "Inserted", rowCount, insertedRowsCount);
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
        LOG.debug("BGInserter{} was signalled to stop, exiting", dataWriteTraceToken);
    }

    @Override
    public String getTaskName()
    {
        return format("BGInserter%s", this.dataWriteTraceToken);
    }

    private VectorSchemaRoot pollNext()
    {
        VectorSchemaRoot vectorSchemaRoot = this.insertChucksSupplier.get();
        if (vectorSchemaRoot != null) {
            LOG.debug("BGInserter{} polled new chunk of {} rows, hash={}", dataWriteTraceToken, vectorSchemaRoot.getRowCount(), vectorSchemaRoot.hashCode());
        }
        else {
            LOG.debug("BGInserter{} polled null", dataWriteTraceToken);
        }
        return vectorSchemaRoot;
    }
}
