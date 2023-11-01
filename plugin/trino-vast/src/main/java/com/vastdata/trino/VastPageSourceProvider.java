/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.vastdata.client.QueryDataPagination;
import com.vastdata.client.QueryDataResponseHandler;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastDebugConfig;
import com.vastdata.client.executor.VastRetryConfig;
import com.vastdata.client.schema.EnumeratedSchema;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
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
import io.trino.spi.predicate.TupleDomain;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.vastdata.trino.VastSessionProperties.getDynamicFilterCompactionThreshold;
import static com.vastdata.trino.VastSessionProperties.getEnableSortedProjections;
import static com.vastdata.trino.VastSessionProperties.getQueryDataRowsPerPage;
import static com.vastdata.trino.VastSessionProperties.getRetryMaxCount;
import static com.vastdata.trino.VastSessionProperties.getRetrySleepDuration;

public class VastPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger LOG = Logger.get(VastPageSourceProvider.class);

    private final VastClient client;

    @Inject
    public VastPageSourceProvider(VastClient client)
    {
        this.client = client;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split,
            ConnectorTableHandle tableHandle, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        VastTransaction tx = (VastTransaction) transactionHandle;
        VastTraceToken traceToken = tx.generateTraceToken(session.getTraceToken());
        String traceStr = traceToken.toString();
        LOG.info("QueryData(%s) createPageSource(%s, %s, %s, %s)", traceStr, transactionHandle, split, tableHandle, columns);
        VastTableHandle table = (VastTableHandle) tableHandle;
        VastSplit vastSplit = (VastSplit) split;
        List<VastColumnHandle> projectedColumns = columns
                .stream()
                .map(VastColumnHandle.class::cast)
                .collect(Collectors.toList());

        TupleDomain<VastColumnHandle> enforcedPredicate = table.getPredicate();

        // Poll current dynamic filter predicate (may return more filters during selective joins)
        // https://trino.io/docs/current/admin/dynamic-filtering.html
        TupleDomain<VastColumnHandle> dynamicPredicate = dynamicFilter
                .getCurrentPredicate()
                .transformKeys(VastColumnHandle.class::cast)
                .simplify(getDynamicFilterCompactionThreshold(session));
        TupleDomain<VastColumnHandle> predicate = enforcedPredicate.intersect(dynamicPredicate);
        if (predicate.isNone()) {
            LOG.debug("QueryData(%s) returning EmptyPageSource", traceStr);
            return new EmptyPageSource();
        }

        List<VastColumnHandle> filteredColumns = predicate
                .getDomains()
                .map(domains -> List.copyOf(domains.keySet()))
                .orElse(List.of());

        // Since the schema is references by projections and predicate FieldIndex expressions, we need to keep its fields ordered
        Set<Field> schemaFields = new LinkedHashSet<>();
        projectedColumns.forEach(vch -> schemaFields.add(vch.getBaseField()));
        filteredColumns.forEach(vch -> schemaFields.add(vch.getBaseField()));

        List<VastSubstringMatch> substringMatches = table.getSubstringMatches();
        substringMatches.forEach(match -> schemaFields.add(match.getColumn().getBaseField()));

        Optional<ComplexPredicate> complexPredicate = Optional.ofNullable(table.getComplexPredicate());
        complexPredicate.ifPresent(pred -> {
            ImmutableSet.Builder<VastColumnHandle> builder = ImmutableSet.builder();
            pred.collectColumns(builder);
            builder.build().forEach(column -> schemaFields.add(column.getBaseField()));
        });

        LOG.debug("schemaFields: %s", schemaFields);
        EnumeratedSchema enumeratedSchema = new EnumeratedSchema(schemaFields);

        TrinoPredicateSerializer predicateSerializer = new TrinoPredicateSerializer(predicate, complexPredicate, substringMatches, enumeratedSchema);
        TrinoProjectionSerializer projectionSerializer = new TrinoProjectionSerializer(projectedColumns, enumeratedSchema);
        List<Integer> projections = projectionSerializer.getProjectionIndices();
        LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>> baseFieldWithProjections = projectionSerializer.getBaseFieldWithProjections();
        LOG.info("QueryData(%s) schema: %s, projections: %s, projectedColumns=%s, filteredColumns=%s", traceToken, enumeratedSchema, projections, projectedColumns, filteredColumns);

        VastDebugConfig debugConfig = new VastDebugConfig(
                VastSessionProperties.getDebugDisableArrowParsing(session),
                VastSessionProperties.getDebugDisablePageQueueing(session));
        VastRetryConfig retryConfig = new VastRetryConfig(getRetryMaxCount(session), getRetrySleepDuration(session));

        QueryDataPagination pagination = new QueryDataPagination(vastSplit.getContext().getNumOfSubSplits());

        List<URI> dataEndpoints = vastSplit.getEndpoints();

        AtomicReference<URI> usedDataEndpoint = new AtomicReference<>();
        int rowsPerPage = getQueryDataRowsPerPage(session);
        if (table.getLimit().isPresent()) {
            long limit = table.getLimit().orElseThrow();
            if (limit < rowsPerPage) {
                rowsPerPage = (int) limit;
            }
        }
        // no need to limit when no projections are specified (e.g. in `SELECT count(*) FROM t`), to use optimized VAST implementation
        Optional<Integer> batchSize = projections.size() > 0 ? Optional.of(rowsPerPage) : Optional.empty();
        Schema responseSchema = projectionSerializer.getResponseSchema();
        Supplier<QueryDataResponseParser> fetchPages = () -> {
            QueryDataResponseSchemaConstructor querySchema = QueryDataResponseSchemaConstructor.deconstruct(traceStr, responseSchema, projections, baseFieldWithProjections);
            AtomicReference<QueryDataResponseParser> result = new AtomicReference<>();
            Supplier<QueryDataResponseHandler> handlerSupplier = () -> {
                QueryDataResponseParser parser = new QueryDataResponseParser(traceToken, querySchema, debugConfig, pagination, table.getLimit());
                result.set(parser);
                return new QueryDataResponseHandler(parser::parse, traceToken);
            };
            client.queryData(
                    tx, traceToken, table.getSchemaName(), table.getTableName(), enumeratedSchema.getSchema(), projectionSerializer, predicateSerializer,
                    handlerSupplier,
                    usedDataEndpoint,
                    vastSplit.getContext(), vastSplit.getSchedulingInfo(),
                    dataEndpoints, retryConfig, batchSize, table.getBigCatalogSearchPath(), pagination,
                    getEnableSortedProjections(session));
            return result.get();
        };

        VastPageSource source = new VastPageSource(traceToken, vastSplit, fetchPages, table.getLimit());
//        if (table.getUpdatable()) {
//            return new VastMergablePageSource();?
//        }
        return source;
    }
}
