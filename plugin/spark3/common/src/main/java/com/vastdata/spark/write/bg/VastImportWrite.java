/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

import com.vastdata.client.QueryDataExtraParams;
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

import static com.vastdata.client.importdata.VastImportDataMetadataUtils.getImportDataHiddenColumnIndex;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.getTablePath;

public class VastImportWrite
        implements VastWriteStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger(VastImportWrite.class);

    private final WriteContext ctx;

    public VastImportWrite(
            Function<VastConfig, VastClient> vastClientSupplier, String dataWriteTraceToken, VastConfig vastConfig, URI endpoint, VastTransaction tx, String schemaName,
            String tableName)
    {
        this.ctx = new WriteContext(vastClientSupplier, dataWriteTraceToken, vastConfig, endpoint, tx, schemaName, tableName, null);
    }

    @Override
    public void write(VectorSchemaRoot nextChunk)
            throws VastException
    {
        VastTraceToken traceToken = ctx.getTx().generateTraceToken(Optional.of("VastBGWriter[Import]"));
        int columnCount = nextChunk.getFieldVectors().size();

        LOG.debug("{} importing data, chuck size: {}, columns: {}, chunk hash: {}",
                ctx.getDataWriteTraceToken(),
                nextChunk.getRowCount(),
                columnCount,
                nextChunk.hashCode()
        );
        final int retryMaxCount = ctx.getVastConfig().getRetryMaxCount();
        final int retrySleepDuration = ctx.getVastConfig().getRetrySleepDuration();
        boolean parallelImport = ctx.getVastConfig().getParallelImport();
        Supplier<RetryStrategy> retryStrategy = () -> RetryStrategyFactory.fixedSleepBetweenRetries(retryMaxCount, retrySleepDuration);

        final int hiddenColumnIndex = getImportDataHiddenColumnIndex(nextChunk.getSchema().getFields());
        List<VectorSchemaRoot> toClose = new ArrayList<>(nextChunk.getRowCount());
        Function<Integer, VectorSchemaRoot> rowSupplier = i -> {
            LOG.debug("{} rowSupplier supplying row no. {} from chunk hash: {}",
                    ctx.getDataWriteTraceToken(), i, nextChunk.hashCode());
            VectorSchemaRoot slice = nextChunk.slice(i, 1);
            toClose.add(slice);
            return slice;
        };
        IntFunction<ImportDataFile> importDataFileIntFunction = new ImportDataFileMapper(rowSupplier, hiddenColumnIndex);
        List<ImportDataFile> sourceFiles = IntStream.range(0, nextChunk.getRowCount())
                .mapToObj(importDataFileIntFunction)
                .collect(Collectors.toList());

        ImportDataContext importCtx = new ImportDataContext(
                sourceFiles, getTablePath(ctx.getSchemaName(), ctx.getTableName()))
                .withChunkLimit(ctx.getVastConfig().getImportChunkLimit());
        try (nextChunk) {
            new ImportDataExecutor<>(ctx.getVastClientSupplier().apply(ctx.getVastConfig()))
                    .execute(importCtx, ctx.getTx(), traceToken, Collections.singletonList(ctx.getEndpoint()),
                            retryStrategy, parallelImport, new QueryDataExtraParams(), null);
        }
        finally {
            toClose.forEach(VectorSchemaRoot::close);
        }
    }
}
