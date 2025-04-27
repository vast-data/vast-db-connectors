/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.importdata;

import com.vastdata.client.ParsedURL;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastResponse;
import com.vastdata.client.VerifyParam;
import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.client.error.ImportDataFailure;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.executor.RetryStrategy;
import com.vastdata.client.executor.WorkFactory;
import com.vastdata.client.executor.WorkLoad;
import com.vastdata.client.schema.ImportDataContext;
import com.vastdata.client.schema.ImportDataFile;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransactionHandleManager;
import io.airlift.log.Logger;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.vastdata.client.error.VastExceptionFactory.ioException;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static java.lang.String.format;

public class ImportDataExecutor<T extends VastTransaction>
{
    private static final Logger LOG = Logger.get(ImportDataExecutor.class);
    private final VastClient client;

    public ImportDataExecutor(VastClient client)
    {
        this.client = client;
    }

    public void execute(ImportDataContext ctx, VastTransactionHandleManager<T> vastTransactionHandleManager, List<URI> dataEndpoints,
            Supplier<RetryStrategy> retryStrategy, boolean parallel)
            throws VastException
    {
        T vastTransaction = vastTransactionHandleManager.startTransaction(new StartTransactionContext(false, true));
        VastTraceToken traceToken = vastTransaction.generateTraceToken(Optional.empty());
        try {
            execute(ctx, vastTransaction, traceToken, dataEndpoints, retryStrategy, parallel);
            vastTransactionHandleManager.commit(vastTransaction);
        }
        catch (VastException failure) {
            LOG.error(failure, "ImportData(%s): procedure ended with failure", traceToken);
            rollbackTransaction(vastTransactionHandleManager, vastTransaction);
            throw failure;
        }
        catch (Throwable any) {
            LOG.error(any, "ImportData(%s): procedure execution failed with exception", traceToken);
            rollbackTransaction(vastTransactionHandleManager, vastTransaction);
            throw ioException(String.format("Failed importing data: %s", any.getMessage()), any);
        }
    }

    public void execute(ImportDataContext ctx, T vastTransaction, VastTraceToken traceToken, List<URI> dataEndpoints, Supplier<RetryStrategy> retryStrategy, boolean parallel)
            throws VastException
    {
        final ParsedURL of = ParsedURL.of(ctx.getDest());
        final String bucket = of.getBucket();
        final String schemaName = of.getFullSchemaPath();
        final String tableName = of.getTableName();
        if (!hasSchema(ctx)) {
            VerifyParam.verify(client.listBuckets(false).contains(bucket), format("ImportData(%s): Bucket %s doesn't exist", traceToken, bucket));
            VerifyParam.verify(client.schemaExists(vastTransaction, schemaName), format("ImportData(%s): Schema name %s doesn't exist", traceToken, schemaName));
            VerifyParam.verify(client.listTables(vastTransaction, schemaName, 1000).anyMatch(tableName::equals), format("ImportData(%s): Table %s doesn't exist", traceToken, tableName));
            final List<Field> fields = client.listColumns(vastTransaction, schemaName, VastImportDataMetadataUtils.getTableNameForAPI(tableName), 1000, Collections.emptyMap());
            final Map<String, Field> fieldsMap = fields.stream().collect(Collectors.toMap(Field::getName, Function.identity()));
            ctx.getSourceFiles().forEach(f -> f.setFieldsDefaultValues(fieldsMap));
        }

        LinkedBlockingQueue<Throwable> importDataFailures = new LinkedBlockingQueue<>();
        ImportDataResponseMapConsumer mapConsumer = new ImportDataResponseMapConsumer();
        Consumer<InputStream> importDataResponseParser = new ImportDataResponseParser(mapConsumer);
        Predicate<VastResponse> successConsumer = response -> {
            try {
                return ImportDataResultHandler.handleResponse(response, mapConsumer.getResult(), traceToken);
            }
            catch (ImportDataFailure e) {
                LOG.error(e, "ImportData(%s): Import data failed", traceToken);
                importDataFailures.add(e);
            }
            return false;
        };
        BiFunction<ImportDataContext, URI, VastResponse> work = (importDataContext, dataEndpoint) -> this.client.importData(vastTransaction, traceToken, importDataContext,
                importDataResponseParser, dataEndpoint);
        if (parallel) {
            List<ImportDataContext> importDataContextsPerFile = chunkifyImportFilesList(ctx, dataEndpoints);

            AtomicBoolean anyFailed = new AtomicBoolean(false);
            BiConsumer<Throwable, URI> exceptionsHandler = (anyException, uri)  -> {
                LOG.error(anyException, "ImportData(%s): Caught exception during execution on endpoint: %s", traceToken, uri);
                if (anyException instanceof VastException || anyException instanceof VastRuntimeException) {
                    // for vast-originated exceptions - uri already should have been part of the exception, keep as is. In case of trouble fix in prior layers
                    importDataFailures.add(anyException);
                }
                else {
                    importDataFailures.add(toRuntime(format("ImportData(%s): Caught exception during execution on endpoint: %s", traceToken, uri), anyException));
                }
                anyFailed.set(true);
            };
            Supplier<Function<URI, VastResponse>> callableSupplier = WorkFactory.fromCollection(importDataContextsPerFile, work);
            WorkLoad<VastResponse> workLoad = new WorkLoad.Builder<VastResponse>()
                    .setWorkSupplier(callableSupplier)
                    .setTraceToken(traceToken)
                    .setWorkConsumers(successConsumer, exceptionsHandler)
                    .setEndpoints(dataEndpoints)
                    .setThreadsPrefix("import-data")
                    .setRetryStrategy(retryStrategy)
                    .setCircuitBreaker(() -> importDataFailures.isEmpty() && !anyFailed.get())
                    .build();
            workLoad.executeWorkLoad();
        }
        else {
            successConsumer.test(work.apply(ctx, dataEndpoints.get(0)));
        }
        if (!importDataFailures.isEmpty()) {
            Throwable firstException = importDataFailures.peek();
            if (firstException instanceof ImportDataFailure) {
                throw (ImportDataFailure) firstException;
            }
            else {
                throw toRuntime(firstException);
            }
        }
    }

    private List<ImportDataContext> chunkifyImportFilesList(ImportDataContext ctx, List<URI> dataEndpoints)
    {
        return ImportDataFileChunkifierFactory.getInstance(ImportDataFileChunkifierStrategy.CHUNK).apply(ctx, dataEndpoints);
    }

    private boolean hasSchema(ImportDataContext ctx)
            throws VastUserException
    {
        List<ImportDataFile> sourceFiles = ctx.getSourceFiles();
        VerifyParam.verify(!sourceFiles.isEmpty(), "Import data files list is empty");
        Map<Boolean, List<ImportDataFile>> partitioned = sourceFiles.stream().collect(Collectors.partitioningBy(ImportDataFile::hasSchemaRoot));
        boolean allHaveSchema = partitioned.get(false).isEmpty();
        boolean allHaveNoSchema = partitioned.get(true).isEmpty();
        VerifyParam.verify(allHaveNoSchema ^ allHaveSchema, "Import data files list is in an illegal state");
        return allHaveSchema;
    }

    private void rollbackTransaction(VastTransactionHandleManager<T> vastTransactionHandleManager, T vastTransaction)
    {
        if (vastTransactionHandleManager.isOpen(vastTransaction)) {
            try {
                vastTransactionHandleManager.rollback(vastTransaction);
            }
            catch (Throwable t) {
                LOG.error(t, "Transaction rollback failed: %s", vastTransaction);
            }
        }
    }
}
