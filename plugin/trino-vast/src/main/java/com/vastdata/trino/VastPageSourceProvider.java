/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.vastdata.ShapingLogger;
import com.vastdata.ShapingLoggerFactory;
import com.vastdata.client.PrefillColumn;
import com.vastdata.client.QueryDataPagination;
import com.vastdata.client.QueryDataResponseHandler;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastDebugConfig;
import com.vastdata.client.executor.VastRetryConfig;
import com.vastdata.client.metrics.DataResponseParserMetrics;
import com.vastdata.client.schema.EnumeratedSchema;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.memory.VastMemoryLimiter;
import com.vastdata.trino.metrics.PageSourceMetrics;
import com.vastdata.trino.predicate.ComplexPredicate;
import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.vastdata.client.importdata.VastImportDataMetadataUtils.BIG_CATALOG_TABLE_NAME;
import static com.vastdata.trino.VastSessionProperties.getCompression;
import static com.vastdata.trino.VastSessionProperties.getDynamicFilterPushdownThreshold;
import static com.vastdata.trino.VastSessionProperties.getEnableSortedProjections;
import static com.vastdata.trino.VastSessionProperties.getMaxNumOfBytesPerColumn;
import static com.vastdata.trino.VastSessionProperties.getQueryDataRowsPerPage;
import static com.vastdata.trino.VastSessionProperties.getRetryMaxCount;
import static com.vastdata.trino.VastSessionProperties.getRetrySleepDuration;
import static java.util.Objects.requireNonNull;

public class VastPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger LOG = Logger.get(VastPageSourceProvider.class);

    private final VastClient client;
    private final VastConfig vastConfig;
    private final ShapingLoggerFactory shapingLoggerFactory;
    private final VastMemoryLimiter memoryLimiter;
    private final DataResponseParserMetrics dataResponseParserMetrics;
    private final PageSourceMetrics vastPageSourceMetrics;
    private final ShapingLogger shapingLogger;

    @Inject
    public VastPageSourceProvider(VastClient client,
                                  ShapingLoggerFactory shapingLoggerFactory,
                                  VastConfig vastConfig,
                                  VastMemoryLimiter memoryLimiter,
                                  DataResponseParserMetrics dataResponseParserMetrics,
                                  PageSourceMetrics vastPageSourceMetrics)
    {
        this.client = requireNonNull(client);
        this.shapingLoggerFactory = requireNonNull(shapingLoggerFactory);
        this.vastConfig = requireNonNull(vastConfig);
        this.memoryLimiter = requireNonNull(memoryLimiter);
        this.dataResponseParserMetrics = requireNonNull(
                dataResponseParserMetrics);
        this.vastPageSourceMetrics = requireNonNull(vastPageSourceMetrics);
        this.shapingLogger = shapingLoggerFactory.getInstance(getClass(), LOG);
    }

    private static long estimateRowSize(List<Field> flattenedFields)
    {
        long sum = flattenedFields
                .stream()
                .map(TypeUtils::convertArrowFieldToTrinoType)
                .mapToLong(t -> t.isFlatVariableWidth() ?
                        40 :
                        t.getFlatFixedSize()) // estimate for variable width types
                .sum();
        return sum == 0 ?
                1 :
                sum; // avoid empty projection (e.g. count(*)) resulting in 0 size
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
                                                ConnectorSession session,
                                                ConnectorSplit split,
                                                ConnectorTableHandle tableHandle,
                                                List<ColumnHandle> columns,
                                                DynamicFilter dynamicFilter)
    {
        final String endUser = session.getUser();
        VastTransaction tx = (VastTransaction) transactionHandle;
        String fullToken = session.getTraceToken().orElse("");
        VastTraceToken traceToken = tx.generateTraceToken(
                Optional.of(fullToken));
        String traceStr = traceToken.toString();
        shapingLogger.debug(
                "QueryData(%s) createPageSource(%s, %s, %s, %s) endUser=%s",
                traceStr, transactionHandle, split, tableHandle, columns,
                endUser);
        VastTableHandle table = (VastTableHandle) tableHandle;
        VastSplit vastSplit = (VastSplit) split;
        List<VastColumnHandle> serverProjectedColumns = columns
                .stream()
                .map(VastColumnHandle.class::cast)
                .collect(Collectors.toList());

        TupleDomain<VastColumnHandle> predicate = vastSplit.getFilters();
        Optional<Map<ColumnHandle, Domain>> dfDomainsOpt = dynamicFilter
                .getCurrentPredicate()
                .getDomains();
        List<String> sortedColumns = table.getSortedColumns().orElse(List.of());

        boolean dfApplied = false;
        if (dfDomainsOpt.isPresent()) {
            TupleDomain<VastColumnHandle> dfTupleDomain;
            if (sortedColumns.isEmpty()) {
                dfTupleDomain = dynamicFilter
                        .getCurrentPredicate()
                        .transformKeys(
                                columnHandle -> (VastColumnHandle) columnHandle);
            }
            else {
                // only non-sorted columns can be filtered by dynamic filters
                Map<VastColumnHandle, Domain> dfVastDomains = dfDomainsOpt
                        .orElseThrow()
                        .entrySet()
                        .stream()
                        .filter(entry -> !sortedColumns.contains(
                                ((VastColumnHandle) entry.getKey())
                                        .getField()
                                        .getName()))
                        .collect(Collectors.toMap(
                                e -> (VastColumnHandle) e.getKey(),
                                Map.Entry::getValue));
                dfTupleDomain = TupleDomain.withColumnDomains(dfVastDomains);
            }
            dfTupleDomain = dfTupleDomain.simplify(
                    getDynamicFilterPushdownThreshold(session));
            TupleDomain<VastColumnHandle> before = predicate;
            predicate = predicate.intersect(dfTupleDomain);
            dfApplied = before.equals(predicate);
        }
        if (predicate.isNone()) {
            shapingLogger.debug("QueryData(%s) returning EmptyPageSource",
                    traceStr);
            return new EmptyPageSource();
        }

        List<VastColumnHandle> filteredColumns = predicate
                .getDomains()
                .map(domains -> List.copyOf(domains.keySet()))
                .orElse(List.of());

        // Since the schema is references by serverProjections and predicate FieldIndex expressions, we need to keep its fields ordered
        Set<Field> schemaFields = new LinkedHashSet<>();
        serverProjectedColumns.forEach(
                vch -> schemaFields.add(vch.getBaseField()));
        filteredColumns.forEach(vch -> schemaFields.add(vch.getBaseField()));

        Optional<Map<VastColumnHandle, Domain>> domains = ((VastSplit) split)
                .getFilters()
                .getDomains();
        List<String> partitionColumnNames = table
                .getPartitionPostTransformColumnNames()
                .orElse(List.of());
        List<VastSubstringMatch> substringMatches = table
                .getSubstringMatches()
                .stream()
                .filter(sm ->
                {
                    boolean isSingleValue = false;
                    if (domains.isPresent()) {
                        Domain domain = domains.orElseThrow().get(sm.column());
                        if (domain != null && domain.isSingleValue()) {
                            isSingleValue = true;
                        }
                    }
                    return !(partitionColumnNames.contains(
                            sm.column().getField().getName()) && isSingleValue);
                })
                .collect(Collectors.toList());
        substringMatches.forEach(
                match -> schemaFields.add(match.column().getBaseField()));

        Optional<ComplexPredicate> complexPredicate = Optional.ofNullable(
                table.getComplexPredicate());
        complexPredicate.ifPresent(pred ->
        {
            ImmutableSet.Builder<VastColumnHandle> builder = ImmutableSet.builder();
            pred.collectColumns(builder);
            builder
                    .build()
                    .forEach(column -> schemaFields.add(column.getBaseField()));
        });
        shapingLogger.debug("QueryData(%s) schemaFields: %s, predicate: %s",
                traceToken, schemaFields, predicate);
        Map<String, String> metadata = new HashMap<>();
        long mstPointer = (((VastTableHandle) tableHandle).getHandleID().getMstPointer());
        if (mstPointer > 0) {
            shapingLogger.debug("adding mstPointer header %s", mstPointer);
            metadata.put("mst_pointer", Long.toString(mstPointer));
        }
        EnumeratedSchema enumeratedSchema = new EnumeratedSchema(schemaFields, metadata);

        List<PrefillColumn<VastColumnHandle>> prefillColumns = calculatePrefillColumns(
                table, (VastSplit) split, session, enumeratedSchema,
                serverProjectedColumns);
        List<VastColumnHandle> tableProjectedColumns = new ArrayList<>(
                serverProjectedColumns);
        // the server does not project prefill columns, but they are needed for constructing the page, so we need to add them to the table projections and remove them from server projections
        serverProjectedColumns.removeAll(prefillColumns
                .stream()
                .map(PrefillColumn::getColumnHandle)
                .toList());

        TrinoPredicateSerializer predicateSerializer = new TrinoPredicateSerializer(
                shapingLoggerFactory, predicate, complexPredicate,
                substringMatches, enumeratedSchema);
        TrinoProjectionSerializer projectionSerializer = new TrinoProjectionSerializer(
                tableProjectedColumns, serverProjectedColumns,
                enumeratedSchema);
        List<Integer> serverProjections = projectionSerializer.getServerProjectionIndices();
        List<Integer> tableProjections = projectionSerializer.getTableProjectionIndices();
        LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>> baseFieldWithProjections = projectionSerializer.getBaseFieldWithProjections();
        shapingLogger.debug(
                "QueryData(%s) schema: %s, serverProjections: %s, tableProjections: %s, serverProjectedColumns=%s, filteredColumns=%s",
                traceToken, enumeratedSchema, serverProjections,
                tableProjections, serverProjectedColumns, filteredColumns);

        VastDebugConfig debugConfig = new VastDebugConfig(
                VastSessionProperties.getDebugDisableArrowParsing(session),
                VastSessionProperties.getDebugDisablePageQueueing(session),
                VastSessionProperties.getEnableServerStatsCollections(session));
        VastRetryConfig retryConfig = new VastRetryConfig(
                getRetryMaxCount(session), getRetrySleepDuration(session));

        QueryDataPagination pagination = new QueryDataPagination(
                vastSplit.getContext().getNumOfSubSplits());

        List<URI> dataEndpoints = vastSplit.getEndpoints();

        int rowsPerPage = getQueryDataRowsPerPage(session);
        if (table.getLimit().isPresent()) {
            long limit = table.getLimit().orElseThrow();
            if (limit < rowsPerPage) {
                rowsPerPage = (int) limit;
            }
        }
        final int expectedRowsPerPage = rowsPerPage;
        // no need to limit when no serverProjections are specified (e.g. in `SELECT count(*) FROM t`), to use optimized VAST implementation
        Schema tableSchema = projectionSerializer.getTableResponseSchema();
        Schema serverSchema = projectionSerializer.getServerResponseSchema();
        Function<Integer, QueryDataResponseParser> fetchPages = (expectedNumOfRows) ->
        {
            shapingLogger.debug(
                    "QueryData(%s): Analyzing schema serverProjections: schema=%s, projectionPaths=%s",
                    traceStr, tableSchema, baseFieldWithProjections);
            QueryDataResponseSchemaConstructor querySchema = QueryDataResponseSchemaConstructor.deconstruct(
                    shapingLoggerFactory, traceStr, serverSchema, tableSchema,
                    serverProjections, tableProjections,
                    baseFieldWithProjections);
            AtomicReference<QueryDataResponseParser> result = new AtomicReference<>();
            Supplier<QueryDataResponseHandler> handlerSupplier = () ->
            {
                QueryDataResponseParser parser = new QueryDataResponseParser(
                        shapingLoggerFactory, vastConfig, traceToken,
                        querySchema, prefillColumns, debugConfig, pagination,
                        table.getLimit(), Optional.of(getMaxNumOfBytesPerColumn(session)));
                result.set(parser);
                return new QueryDataResponseHandler(shapingLoggerFactory,
                        parser::parse, traceToken);
            };
            int numOfRows = Math.min(expectedNumOfRows, Math.min(VastSessionProperties.getQueryDataRowsPerPage(session), expectedRowsPerPage));
            Optional<Integer> finalBatchSize = serverProjections.isEmpty() ? Optional.empty() : Optional.of(numOfRows);
            shapingLogger.debug("QueryData(%s) setting page size to %s rows",
                    traceToken, finalBatchSize);
            client.queryData(tx, traceToken, table.getSchemaName(),
                    table.getTableName(), enumeratedSchema.getSchema(),
                    projectionSerializer, predicateSerializer, handlerSupplier,
                    vastSplit.getContext(), vastSplit.getSchedulingInfo(),
                    dataEndpoints, retryConfig, finalBatchSize,
                    table.getBigCatalogSearchPath(), pagination,
                    getEnableSortedProjections(session),
                    getCompression(session), table.getExtraQueryParams(),
                    endUser);
            return result.get();
        };
        Map<String, Long> additionalMetrics = Map.of("df_applied",
                dfApplied ? 1L : 0, "serverProjectedColumns",
                (long) serverProjections.size());
        long estimatedRowsSize = estimateRowSize(tableSchema.getFields());
        return new VastPageSource(traceToken, memoryLimiter, session, vastSplit,
                expectedRowsPerPage, estimatedRowsSize, fetchPages, table
                        .getLimit(), additionalMetrics,
                dataResponseParserMetrics, vastPageSourceMetrics);
    }

    private List<PrefillColumn<VastColumnHandle>> calculatePrefillColumns(
            VastTableHandle table,
            VastSplit split,
            ConnectorSession session,
            EnumeratedSchema enumeratedSchema,
            List<VastColumnHandle> projectedColumns)
    {
        if (!VastSessionProperties.getEnablePrefillOptimization(
                session) || projectedColumns
                .stream()
                .anyMatch(vch -> vch
                        .getBaseField()
                        .getType()
                        .isComplex()) || table
                .getBigCatalogSearchPath()
                .isPresent() || table
                .getTableName()
                .contains(BIG_CATALOG_TABLE_NAME)) {
            return Collections.emptyList();
        }
        List<PrefillColumn<VastColumnHandle>> ret = Collections.emptyList();
        if (split.getFilters().getDomains().isPresent()) {
            ret = split
                    .getFilters()
                    .getDomains()
                    .orElseThrow()
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().isSingleValue())
                    .filter(entry -> !entry
                            .getKey()
                            .getBaseField()
                            .getType()
                            .isComplex())
                    .filter(entry -> projectedColumns.contains(entry.getKey()))
                    .map(entry -> new PrefillColumn<>(
                            enumeratedSchema.getBaseFieldIndexByName(
                                    entry.getKey().getField().getName()),
                            entry.getKey(), entry.getValue().getSingleValue()))
                    .collect(Collectors.toList());
        }
        return ret;
    }
}
