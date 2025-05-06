/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.QueryDataPagination;
import com.vastdata.client.QueryDataResponseHandler;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastDebugConfig;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.VastSplitContext;
import com.vastdata.client.error.VastException;
import com.vastdata.client.executor.VastRetryConfig;
import com.vastdata.client.schema.EnumeratedSchema;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.vastdata.trino.VastSessionProperties.getDataEndpoints;
import static com.vastdata.trino.VastSessionProperties.getRetryMaxCount;
import static com.vastdata.trino.VastSessionProperties.getRetrySleepDuration;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;



public class GetTableSizeHelper
{
    private static final VastSplitContext getSizeContext = new VastSplitContext(0xffffffffL - 3, 1, 1, 1);
    private static final EnumeratedSchema emptyEnumeratedSchema = new EnumeratedSchema(emptySet());

    public static long getTableSizeEstimate(final VastTableHandle table,
                                            final TupleDomain<VastColumnHandle> predicates, final VastTransaction tx,
                                            final VastTraceToken token, final VastSchedulingInfo schedulingInfo,
                                            final VastClient client,
                                            final ConnectorSession session)
            throws VastException
    {
        final String trace = "getTableSize:" + table.getTableName();
        final List<URI> endpoints = getDataEndpoints(session);
        final VastSplit split = new VastSplit(endpoints, getSizeContext, schedulingInfo);
        final QueryDataPagination pagination = new QueryDataPagination(split.getContext().getNumOfSubSplits());
        final List<Field> fields = (predicates.getDomains().orElseThrow()).entrySet().stream()
            .filter(entry -> !entry.getValue().isAll())
            .map(entry -> entry.getKey().getField())
            .collect(Collectors.toList());
        final EnumeratedSchema enumeratedSchema = new EnumeratedSchema(fields);
        final TrinoPredicateSerializer predicateSerializer =
            new TrinoPredicateSerializer(predicates, Optional.empty(), emptyList(), enumeratedSchema);
        final VastDebugConfig debugConfig = new VastDebugConfig(
                VastSessionProperties.getDebugDisableArrowParsing(session),
                VastSessionProperties.getDebugDisablePageQueueing(session));
        final VastRetryConfig retryConfig = new VastRetryConfig(getRetryMaxCount(session), getRetrySleepDuration(session));

        final Supplier<QueryDataResponseParser> fetchPages = () -> {
            final QueryDataResponseSchemaConstructor querySchema = 
            QueryDataResponseSchemaConstructor.deconstruct(trace, new Schema(emptyList()), emptyList(), new LinkedHashMap<>());
            final AtomicReference<QueryDataResponseParser> result = new AtomicReference<>();
            final Supplier<QueryDataResponseHandler> handlerSupplier = () -> {
                QueryDataResponseParser parser =
                    new QueryDataResponseParser(token, querySchema, debugConfig, pagination, Optional.of(1L));
                result.set(parser);
                return new QueryDataResponseHandler(parser::parse, token);
            };
            client.queryData(
                    tx, token, table.getSchemaName(), table.getTableName(),
                    enumeratedSchema.getSchema(), getTrinoProjectionSerializer(), predicateSerializer,
                    handlerSupplier,
                    new AtomicReference<>(),
                    split.getContext(), split.getSchedulingInfo(),
                    endpoints, retryConfig, Optional.empty(), Optional.empty(), pagination,
                    false, emptyMap());
            return result.get();
        };
        try(final VastPageSource source = new VastPageSource(token, split, fetchPages, Optional.of(1L))) {
            final Page page = source.getNextPage();
            return (long) page.getPositionCount() * (1L << 16);
        }
    }

    private static TrinoProjectionSerializer getTrinoProjectionSerializer()
    {
        return new TrinoProjectionSerializer(emptyList(), emptyEnumeratedSchema);
    }
}
