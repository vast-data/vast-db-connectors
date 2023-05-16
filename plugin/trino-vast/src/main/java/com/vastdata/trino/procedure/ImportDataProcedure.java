/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.procedure;

import com.google.common.collect.ImmutableList;
import com.vastdata.client.ParsedURL;
import com.vastdata.client.VastClient;
import com.vastdata.client.error.ErrorType;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastIOException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.executor.RetryStrategy;
import com.vastdata.client.executor.RetryStrategyFactory;
import com.vastdata.client.importdata.ImportDataExecutor;
import com.vastdata.client.schema.ImportDataContext;
import com.vastdata.trino.TypeUtils;
import com.vastdata.trino.VastTrinoSchemaAdaptor;
import com.vastdata.trino.tx.VastTransactionHandle;
import com.vastdata.trino.tx.VastTrinoTransactionHandleManager;
import io.airlift.log.Logger;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.vastdata.trino.VastSessionProperties.getDataEndpoints;
import static com.vastdata.trino.VastSessionProperties.getImportChunkLimit;
import static com.vastdata.trino.VastSessionProperties.getParallelImport;
import static com.vastdata.trino.VastSessionProperties.getRetryMaxCount;
import static com.vastdata.trino.VastSessionProperties.getRetrySleepDuration;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.PROCEDURE_CALL_FAILED;
import static io.trino.spi.block.MethodHandleUtil.methodHandle;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class ImportDataProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle IMPORT_DATA = methodHandle(
            ImportDataProcedure.class,
            "importData",
            ConnectorSession.class,
            ConnectorAccessControl.class,
            List.class,
            Map.class,
            String.class);

    private static final Logger LOG = Logger.get(ImportDataProcedure.class);

    private final ImportDataExecutor<VastTransactionHandle> importDataExecutor;

    private final VastTrinoTransactionHandleManager transactionHandleManager;
    private final VastTrinoSchemaAdaptor schemaAdaptor = new VastTrinoSchemaAdaptor();

    @Inject
    public ImportDataProcedure(VastClient client, VastTrinoTransactionHandleManager transactionHandleManager)
    {
        this.transactionHandleManager = transactionHandleManager;
        importDataExecutor = new ImportDataExecutor<>(client);
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "rpc",
                "import_data",
                ImmutableList.<Procedure.Argument>builder()
                        .add(new Procedure.Argument("COLUMNS", new ArrayType(VARCHAR)))
                        .add(new Procedure.Argument("SRC", new MapType(VARCHAR, new ArrayType(VARCHAR), TypeUtils.TYPE_OPERATORS)))
                        .add(new Procedure.Argument("DST", VARCHAR))
                        .build(),
                IMPORT_DATA.bindTo(this));
//        without partitions: CALL vast.rpc.import_data(ARRAY[], MAP(ARRAY['<source_file>'], ARRAY[ARRAY[]]), '<dest_url>')
//        with partitions: CALL rpc.import_data(ARRAY['<partition_column>'], MAP(ARRAY['<source_file>'], ARRAY[ARRAY['<partition_value']]), '<dest_url>')
    }

    // called via reflection
    @SuppressWarnings("unused")
    public void importData(ConnectorSession session, ConnectorAccessControl accessControl, List<String> columns, Map<String, List<String>> src, String dst)
    {
        validateColumns(columns);
        List<String> lowerCaseColumnsNames = columns.stream().map(String::toLowerCase).collect(Collectors.toList());
        validateDest(dst);
        String destLowerCase = dst.toLowerCase(Locale.getDefault());
        validateSrc(src);
        LOG.info("importData(%s, %s, %s)", lowerCaseColumnsNames, src, destLowerCase);
        try {
            ImportDataContext ctx = schemaAdaptor.adaptForImportData(lowerCaseColumnsNames, src, destLowerCase).withChunkLimit(getImportChunkLimit(session));
            int retryMaxCount = getRetryMaxCount(session);
            int retrySleepDuration = getRetrySleepDuration(session);
            boolean parallelImport = getParallelImport(session);
            int limit = getImportChunkLimit(session);
            Supplier<RetryStrategy> retryStrategy = () -> RetryStrategyFactory.fixedSleepBetweenRetries(retryMaxCount, retrySleepDuration);
            importDataExecutor.execute(ctx.withChunkLimit(limit), this.transactionHandleManager, getDataEndpoints(session), retryStrategy, parallelImport);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid method params");
        }
        catch (VastIOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
        catch (VastException e) {
            throw new TrinoException(fromVastErrorType(e.getErrorType()), e);
        }
    }

    private ErrorCodeSupplier fromVastErrorType(ErrorType errorType)
    {
        switch (errorType) {
            case USER:
                return GENERIC_USER_ERROR;
            case SERVER:
                return PROCEDURE_CALL_FAILED;
            case CLIENT:
            case GENERAL:
            default:
                return GENERIC_INTERNAL_ERROR;
        }
    }

    private void validateColumns(List<String> columns)
    {
        if (columns == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Missing columns list");
        }
    }

    private void validateSrc(Map<String, List<String>> src)
    {
        if (src == null || src.isEmpty()) {
            LOG.error("Missing source files list");
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Missing source files list");
        }
    }

    private void validateDest(String dst)
    {
        try {
            ParsedURL url = ParsedURL.of(dst);
            if (url.isBaseUrl() || url.isBucketURL() || url.isSchemaURL()) {
                LOG.error("Not a valid destination URL");
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid destination");
            }
        }
        catch (VastUserException ue) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Missing destination param");
        }
    }
}
