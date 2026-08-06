/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver.handle;

import com.sun.net.httpserver.HttpExchange;
import com.vastdata.client.ParsedURL;
import com.vastdata.client.RequestsHeaders;
import com.vastdata.mockserver.MockMapSchema;
import com.vastdata.mockserver.MockTable;
import com.vastdata.mockserver.MockView;
import io.airlift.log.Logger;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.net.URI;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static com.vastdata.client.partition.PartitionConstants.PIT_NAME_SUFFIX;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_DEC128_FIELD;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_UINT64_FIELD;
import static java.lang.String.format;

public class PostHandler
        extends AbstractRequestConsumer
{
    private static final Logger LOG = Logger.get(PostHandler.class);

    public PostHandler(Map<String, Set<MockMapSchema>> schema,
            Set<String> openTransactions)
    {
        super(schema, openTransactions);
    }

    @Override
    protected void handle(HttpExchange he)
            throws Exception
    {
        URI requestURI = he.getRequestURI();
        String query = requestURI.getQuery();
        LOG.info(format("POST %s", requestURI));
        ParsedURL parsedURL = ParsedURL.of(requestURI.getPath());
        Random random = new Random();
        if (parsedURL.isBaseUrl()) {
            if (getTransactionHeader(he).isPresent()) {
                LOG.error("POST for base url already includes transaction ID");
                respondError(
                        "POST for base url already includes transaction ID",
                        he);
            }
            else {
                long i = random.nextLong();
                String txid = Long.toUnsignedString(i);
                openTransactions.add(txid);
                he
                        .getResponseHeaders()
                        .add(RequestsHeaders.TABULAR_TRANSACTION_ID.getHeaderName(),
                                txid);
                respondOK(he);
            }
        }
        else if (parsedURL.isBucketURL()) {
            String bucket = parsedURL.getBucket();
            if (!schemaMap.containsKey(bucket)) {
                LOG.info(
                        "Bucket %s does not exist, will create bucket with empty schema",
                        bucket);
                schemaMap.put(bucket, new HashSet<>());
            }
            else {
                LOG.info("Bucket %s already exists, will not create", bucket);
            }
            respond("", he, 200);
        }
        else if (parsedURL.isSchemaURL()) {
            String bucket = parsedURL.getBucket();
            if (!schemaMap.containsKey(bucket)) {
                respond(format("Bucket %s does not exist", bucket), he, 404);
            }
            else {
                if (query.equals("schema")) {
                    String schemaName = parsedURL.getSchemaName();
                    LOG.info("Handling create schema %s for bucket %s",
                            schemaName, bucket);
                    Set<MockMapSchema> bucketSchemas = schemaMap.get(bucket);
                    if (bucketSchemas == null) {
                        LOG.info("Initializing schemas set");
                        bucketSchemas = new HashSet<>();
                    }
                    bucketSchemas.add(MockMapSchema.empty(schemaName));
                    schemaMap.put(bucket, bucketSchemas);
                    respond("", he, 200);
                }
                else {
                    LOG.info("Not a valid parsedURL");
                    respondError(he);
                }
            }
        }
        else if (parsedURL.hasTable()) {
            String bucket = parsedURL.getBucket();
            if (!schemaMap.containsKey(bucket)) {
                respond(format("Bucket %s does not exist", bucket), he, 404);
            }
            switch (query) {
                case "view":
                case "table": {
                    String schemaName = parsedURL.getSchemaName();
                    String tableName = parsedURL.getTableName();
                    Set<MockMapSchema> mockMapSchemas = schemaMap.get(bucket);
                    Optional<MockMapSchema> existingSchema = mockMapSchemas
                            .stream()
                            .filter(mockSchema -> mockSchema
                                    .getName()
                                    .equals(schemaName))
                            .findAny();
                    if (existingSchema.isPresent()) {
                        if (query.equals("view")) {
                            LOG.info("Handling create view %s for schema %s",
                                    tableName, schemaName);
                            byte[] bytes = readAllBytes(he.getRequestBody());
                            MockSchemaUtil.ParsedViewData parsedViewData = MockSchemaUtil.deserializeCreateViewRequestBody(
                                    bytes);
                            VectorSchemaRoot viewDetails = parsedViewData.getViewDetails();
                            Schema viewDataSchema = parsedViewData.getViewDataSchema();
                            MockView mockView = new MockView(tableName,
                                    viewDetails, viewDataSchema);
                            existingSchema
                                    .get()
                                    .getViews()
                                    .put(tableName, mockView);
                            respondOK(he);
                        }
                        else {
                            LOG.info("Handling create table %s for schema %s",
                                    tableName, schemaName);
                            Map<String, Field> tableColumnsMap = new LinkedHashMap<>();
                            Schema schema = MockSchemaUtil
                                    .parseTableSchema(readAllBytes(he
                                            .getRequestBody()));
                            List<Field> fields = schema.getFields();
                            fields.forEach(
                                    f -> tableColumnsMap.put(f.getName(), f));
                            MockTable mockTable = MockTable.withColumns(
                                    tableName, tableColumnsMap);
                            boolean isPartitionedTable = schema
                                    .getCustomMetadata() != null && schema
                                            .getCustomMetadata()
                                            .containsKey(
                                                    "VAST:table:partition-key-0");
                            if (isPartitionedTable) {
                                String pitTableName = tableName + PIT_NAME_SUFFIX;
                                Map<String, Field> pitFields = new LinkedHashMap<>();
                                pitFields
                                        .put("notARealPitSchema", Field
                                                .nullable("notARealPitSchema",
                                                        new ArrowType.Int(32,
                                                                true)));
                                existingSchema
                                        .get()
                                        .getTables()
                                        .put(pitTableName, MockTable
                                                .withColumns(pitTableName, pitFields));
                            }

                            existingSchema
                                    .get()
                                    .getTables()
                                    .put(tableName, mockTable);
                            respondOK(he);
                        }
                    }
                    else {
                        respond(format("Schema %s does not exist in bucket %s",
                                schemaName, bucket), he, 404);
                    }
                    break;
                }
                case "column": {
                    String schemaName = parsedURL.getSchemaName();
                    String tableName = parsedURL.getTableName();
                    Set<MockMapSchema> mockMapSchemas = schemaMap.get(bucket);
                    Optional<MockMapSchema> existingSchema = mockMapSchemas
                            .stream()
                            .filter(mockSchema -> mockSchema
                                    .getName()
                                    .equals(schemaName))
                            .findAny();
                    if (existingSchema.isPresent()) {
                        if (!existingSchema
                                .get()
                                .getTables()
                                .containsKey(tableName)) {
                            respond(format(
                                    "Table %s does not exist in schema %s",
                                    tableName, schemaName), he, 404);
                        }
                        else {
                            List<Field> newFields = MockSchemaUtil.parseTableFields(
                                    readAllBytes(he.getRequestBody()));
                            Map<String, Field> existingColumns = existingSchema
                                    .get()
                                    .getTables()
                                    .get(tableName)
                                    .getColumns();
                            for (Field newField : newFields) {
                                if (existingColumns.containsKey(
                                        newField.getName())) {
                                    respond(format(
                                            "Column %s already exists in table %s",
                                            newField, tableName), he, 409);
                                    return;
                                }
                            }
                            newFields.forEach(
                                    f -> existingColumns.put(f.getName(), f));
                            respondOK(he);
                        }
                    }
                    else {
                        respond(format("Schema %s does not exist in bucket %s",
                                schemaName, bucket), he, 404);
                    }
                    break;
                }
                case "rows": {
                    String schemaName = parsedURL.getSchemaName();
                    String tableName = parsedURL.getTableName();
                    Set<MockMapSchema> mockMapSchemas = schemaMap.get(bucket);
                    Optional<MockMapSchema> existingSchema = mockMapSchemas
                            .stream()
                            .filter(mockSchema -> mockSchema
                                    .getName()
                                    .equals(schemaName))
                            .findAny();
                    if (existingSchema.isPresent()) {
                        MockTable mockTable = existingSchema
                                .get()
                                .getTables()
                                .get(tableName);
                        if (mockTable == null) {
                            respond(format(
                                    "Table %s does not exist in schema %s/%s",
                                    tableName, schemaName, bucket), he, 404);
                        }
                        else {
                            byte[] requestBody = readAllBytes(he.getRequestBody());
                            int rowCount = MockSchemaUtil.parseRowCountFromArrowIpc(requestBody);
                            Field rowIdField = mockTable.isSorted()
                                    ? ROW_ID_DEC128_FIELD
                                    : ROW_ID_UINT64_FIELD;
                            byte[] rowIdsResponse = MockSchemaUtil.buildRowIdsArrowIpc(
                                    rowCount, rowIdField);
                            respond(rowIdsResponse, he, 200);
                        }
                    }
                    break;
                }
            }
        }
        else {
            LOG.info("Not a valid parsedURL");
            respondError(he);
        }
    }
}
