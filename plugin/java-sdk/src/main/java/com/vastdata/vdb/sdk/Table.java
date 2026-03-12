/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.vdb.sdk;

import com.vastdata.client.ArrowQueryDataSchemaHelper;
import com.vastdata.client.QueryDataPagination;
import com.vastdata.client.QueryDataResponseHandler;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastDebugConfig;
import com.vastdata.client.VastExpressionSerializer;
import com.vastdata.client.VastSplitContext;
import com.vastdata.client.error.VastException;
import com.vastdata.client.rowid.TableType;
import com.vastdata.client.schema.ArrowSchemaUtils;
import com.vastdata.client.schema.EnumeratedSchema;
import com.vastdata.client.tx.VastTraceToken;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class Table
{
    private static final Logger LOG = LoggerFactory.getLogger(Table.class);

    private final String schemaName;
    private final String tableName;
    private final EmptyVectorAdaptorFactory vectorAdaptorFactory;
    private final VastClient client;
    private final RetryConfig retryConfig;
    private Schema schema;
    private Schema schemaWithRowId;
    private final VastTraceToken token;
    private final List<URI> dataEndpoints;

    // For query execution
    private CalciteSerializer calciteSerializer;
    private QueryDataPagination pagination;
    private List<String> columnNames;
    private Schema tableProjectionSchema;
    private boolean hasFinished;
    private Optional<Integer> limit;

    Table(String schemaName, String tableName, VastClient client, List<URI> dataEndpoints, RetryConfig retryConfig)
    {
	// TODO remove comment
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.client = client;
        this.dataEndpoints = dataEndpoints;
        this.retryConfig = retryConfig;

        this.vectorAdaptorFactory = new EmptyVectorAdaptorFactory();

        Optional<String> userTraceToken = Optional.of(schemaName + "/" + tableName);
        this.token = new VastTraceToken(userTraceToken, 0, 0);
    }

    Table(String statement, Schema tableSchema, String schemaName, String tableName, VastClient client, List<URI> dataEndpoints, RetryConfig retryConfig)
    {
        this(schemaName, tableName, client, dataEndpoints, retryConfig);
        schema = tableSchema;
        this.calciteSerializer = new CalciteSerializer(new EnumeratedSchema(schema.getFields()), statement);
        this.limit = calciteSerializer.getLimit();
        this.pagination = new QueryDataPagination(1);
        this.columnNames = calciteSerializer.getProjectedColumns();
        this.tableProjectionSchema = new Schema(schema.getFields().stream().filter(field -> columnNames.contains(field.getName())).toList());
    }

    public void loadSchema()
            throws NoExternalRowIdColumnException, RuntimeException
    {
        List<Field> fields;
        try {
            fields = this.client.listColumns(null,
                    this.schemaName,
                    this.tableName,
                    1000,
                    Collections.emptyMap(),
                    null);
        }
        catch (VastException e) {
            throw new RuntimeException(e);
        }
        this.schemaWithRowId = new Schema(List.copyOf(fields));

        boolean hadExternalRowIdColumn = fields.removeIf(field -> field.getName().equals(ArrowSchemaUtils.VASTDB_EXTERNAL_ROW_ID_COLUMN_NAME));
        if (!hadExternalRowIdColumn) {
            throw new NoExternalRowIdColumnException();
        }
        this.schema = new Schema(fields);
    }

    public Schema getSchema()
            throws TableSchemaNotLoadedException
    {
        if (schema == null) {
            throw new TableSchemaNotLoadedException();
        }

        return schema;
    }

    public VectorSchemaRoot get(ArrayList<String> columnNames, long rowid)
            throws TableSchemaNotLoadedException
    {
        RowIDPredicateSerializer rowIDPredicateSerializer = new RowIDPredicateSerializer(rowid);
        try{
            return getResult(columnNames, rowIDPredicateSerializer);
        }
        finally {
            hasFinished = true;
        }
    }

    private VectorSchemaRoot getResult(List<String> columnNames, VastExpressionSerializer predicateSerializer)
            throws TableSchemaNotLoadedException
    {
        LOG.debug("Table.get for {}.{}" , this.schemaName, this.tableName);

        if (schemaWithRowId == null) {
            throw new TableSchemaNotLoadedException();
        }

        QueryDataPagination pagination = new QueryDataPagination(1);
        VastDebugConfig debugConfig = VastDebugConfig.DEFAULT;
        RootAllocator allocator = new RootAllocator();
        Schema projectionSchema;
        if (columnNames != null) {
            projectionSchema = new Schema(schemaWithRowId.getFields().stream().filter(field -> columnNames.contains(field.getName())).toList());
        }
        else {
            projectionSchema = this.schemaWithRowId;
        }

        final AtomicReference<QueryDataResponseParser> result = new AtomicReference<>();
        Supplier<QueryDataResponseHandler> handlerSupplier = () -> {
            ArrowQueryDataSchemaHelper schemaHelper = ArrowQueryDataSchemaHelper.deconstruct(token, projectionSchema, this.vectorAdaptorFactory);
            QueryDataResponseParser parser = new QueryDataResponseParser(token, schemaHelper, debugConfig, pagination, Optional.empty(), allocator);
            result.set(parser);

            return new QueryDataResponseHandler(parser::parse, token);
        };

        ProjectionSerializer projections = new ProjectionSerializer(
                projectionSchema,
                new EnumeratedSchema(schemaWithRowId.getFields()));

        client.queryData(null,
                token,
                schemaName,
                tableName,
                schemaWithRowId,
                projections,
                predicateSerializer,
                handlerSupplier,
                null,
                new VastSplitContext(0, 1, 1, 1),
                null,
                dataEndpoints,
                this.retryConfig.toVastRetryConfig(),
                Optional.empty(),
                Optional.empty(),
                new QueryDataPagination(1),
                false,
                0,
                Collections.emptyMap(),
                null);

        return result.get().next();
    }

    public VectorSchemaRoot put(VectorSchemaRoot recordBatch)
            throws VastException
    {
        Random random = new Random();
        URI randomDataEndpoint = dataEndpoints.get(random.nextInt(dataEndpoints.size()));

        LOG.debug("Table.put for {}.{} with endpoint {}" , this.schemaName, this.tableName, randomDataEndpoint);

        return client.insertRows(null, schemaName, tableName, recordBatch, randomDataEndpoint,
                Optional.empty(), true, null, TableType.REGULAR);
    }

    VectorSchemaRoot get()
    {
        VastDebugConfig debugConfig = VastDebugConfig.DEFAULT;
        RootAllocator allocator = new RootAllocator();

        final AtomicReference<QueryDataResponseParser> parserAtomicReference = new AtomicReference<>();
        Supplier<QueryDataResponseHandler> handlerSupplier = () -> {
            ArrowQueryDataSchemaHelper schemaHelper = ArrowQueryDataSchemaHelper.deconstruct(token, tableProjectionSchema, this.vectorAdaptorFactory);
            QueryDataResponseParser parser = new QueryDataResponseParser(token, schemaHelper, debugConfig, pagination, Optional.empty(), allocator);
            parserAtomicReference.set(parser);

            return new QueryDataResponseHandler(parser::parse, token);
        };

        ProjectionSerializer projections = new ProjectionSerializer(
                tableProjectionSchema,
                new EnumeratedSchema(schema.getFields()));

        client.queryData(null,
                token,
                schemaName,
                tableName,
                schema,
                projections,
                calciteSerializer,
                handlerSupplier,
                null,
                new VastSplitContext(0, 1, 1, 1),
                null,
                dataEndpoints,
                this.retryConfig.toVastRetryConfig(),
                limit,
                Optional.empty(),
                pagination,
                false,
                0,
                Collections.emptyMap(),
                null);
        VectorSchemaRoot vectorSchemaRoot = parserAtomicReference.get().next();
        if (parserAtomicReference.get().isSplitFinished()) {
            hasFinished = true;
        }
        limit = limit.map(lim -> lim - vectorSchemaRoot.getRowCount());
        return vectorSchemaRoot;
    }

    boolean isFinished()
    {
        return hasFinished;
    }
}
