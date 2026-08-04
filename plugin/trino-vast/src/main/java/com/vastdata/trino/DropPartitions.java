/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.ShapingLoggerFactory;
import com.vastdata.client.QueryDataPagination;
import com.vastdata.client.QueryDataResponseHandler;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastDebugConfig;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.VastSplitContext;
import com.vastdata.client.executor.VastRetryConfig;
import com.vastdata.client.partition.PartitionConstants;
import com.vastdata.client.schema.EnumeratedSchema;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import java.net.URI;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

final class DropPartitions
{
    private static final VastTrinoExceptionFactory vastTrinoExceptionFactory = new VastTrinoExceptionFactory();

    private DropPartitions()
    {
    }

    public static OptionalLong drop(ConnectorSession session,
                                    VastTableHandle table,
                                    VastClient client,
                                    VastTransactionHandle transactionHandle,
                                    VastConfig vastConfig,
                                    BufferAllocator allocator,
                                    ShapingLoggerFactory shapingLoggerFactory)
    {
        try (VectorSchemaRoot resultRoot = getPartitionsToDelete(session, table,
                client, transactionHandle, vastConfig, allocator,
                shapingLoggerFactory)) {
            if (resultRoot.getRowCount() == 0) {
                return OptionalLong.of(0);
            }

            String baseTableName = table
                    .getTableName()
                    .substring(0, table
                            .getTableName()
                            .length() - PartitionConstants.PIT_NAME_SUFFIX.length());
            client.dropPartitionsNonAcid(table.getSchemaName(), baseTableName,
                    resultRoot, session.getUser());
            return OptionalLong.of(resultRoot.getRowCount());
        }
        catch (Exception e) {
            throw vastTrinoExceptionFactory.fromThrowable(e);
        }
    }

    public static VectorSchemaRoot getPartitionsToDelete(ConnectorSession session,
                                                         VastTableHandle table,
                                                         VastClient client,
                                                         VastTransactionHandle transactionHandle,
                                                         VastConfig vastConfig,
                                                         BufferAllocator allocator,
                                                         ShapingLoggerFactory shapingLoggerFactory)
    {
        VastTraceToken traceToken = transactionHandle.generateTraceToken(
                session.getTraceToken());

        List<VastColumnHandle> columnHandles = table.getColumnHandlesCache();
        List<VastColumnHandle> partitionColumnHandles = columnHandles
                .stream()
                .filter(VastColumnHandle::isPartitionKey)
                .collect(Collectors.toList());

        Schema projectedSchema = new Schema(partitionColumnHandles
                .stream()
                .map(VastColumnHandle::getBaseField)
                .collect(Collectors.toList()));

        EnumeratedSchema enumeratedSchema = new EnumeratedSchema(
                new LinkedHashSet<>(projectedSchema.getFields()));

        TrinoPredicateSerializer predicateSerializer = new TrinoPredicateSerializer(
                shapingLoggerFactory, table.getPredicate(),
                Optional.ofNullable(table.getComplexPredicate()),
                table.getSubstringMatches(), enumeratedSchema);

        TrinoProjectionSerializer projectionSerializer = new TrinoProjectionSerializer(
                partitionColumnHandles, partitionColumnHandles,
                enumeratedSchema);

        VastSplitContext context = new VastSplitContext(0, 1, 1, 1);
        VastSchedulingInfo schedulingInfo = client.getSchedulingInfo(
                transactionHandle, traceToken, table.getSchemaName(),
                table.getTableName(), session.getUser());
        List<URI> dataEndpoints = VastSessionProperties.getDataEndpoints(
                session);
        VastRetryConfig retryConfig = new VastRetryConfig(
                VastSessionProperties.getRetryMaxCount(session),
                VastSessionProperties.getRetrySleepDuration(session));

        QueryDataPagination pagination = new QueryDataPagination(1);
        VastDebugConfig debugConfig = new VastDebugConfig(false, false, false);

        QueryDataResponseSchemaConstructor querySchema = QueryDataResponseSchemaConstructor.deconstruct(
                shapingLoggerFactory, traceToken.toString(),
                projectionSerializer.getServerResponseSchema(),
                projectionSerializer.getTableResponseSchema(),
                projectionSerializer.getServerProjectionIndices(),
                projectionSerializer.getTableProjectionIndices(),
                projectionSerializer.getBaseFieldWithProjections());

        QueryDataResponseParser parser = new QueryDataResponseParser(
                shapingLoggerFactory, vastConfig, traceToken, querySchema,
                List.of(), debugConfig, pagination, Optional.empty(), Optional.empty());

        Supplier<QueryDataResponseHandler> handlerSupplier = () -> new QueryDataResponseHandler(
                shapingLoggerFactory, parser::parse, traceToken);

        client.queryData(transactionHandle, traceToken, table.getSchemaName(),
                table.getTableName(), enumeratedSchema.getSchema(),
                projectionSerializer, predicateSerializer, handlerSupplier,
                context, schedulingInfo, dataEndpoints, retryConfig,
                Optional.empty(), Optional.empty(), pagination, false, 0,
                table.getExtraQueryParams(), session.getUser());

        VectorSchemaRoot resultRoot = VectorSchemaRoot.create(projectedSchema,
                allocator);
        resultRoot.allocateNew();
        VastRecordBatchBuilder batchBuilder = new VastRecordBatchBuilder(
                projectedSchema, allocator);

        while (parser.hasNext()) {
            SourcePage page = parser.next();
            if (page != null && page.getPositionCount() > 0) {
                try (VectorSchemaRoot batchRoot = batchBuilder.build(
                        page.getPage())) {
                    VectorSchemaRootAppender.append(resultRoot, batchRoot);
                }
            }
        }
        return resultRoot;
    }
}
