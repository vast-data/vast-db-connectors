/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver.handle;

import com.google.common.collect.Iterables;
import com.google.flatbuffers.FlatBufferBuilder;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.vastdata.client.ParsedURL;
import com.vastdata.mockserver.MockListBucketsReply;
import com.vastdata.mockserver.MockMapSchema;
import com.vastdata.mockserver.MockTable;
import io.airlift.log.Logger;
import org.apache.arrow.vector.types.pojo.Field;
import vast_flatbuf.tabular.ListSchemasResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static java.lang.String.format;

public class GetHandler
        extends AbstractRequestConsumer
{
    private static final Logger LOG = Logger.get(GetHandler.class);

    public GetHandler(Map<String, Set<MockMapSchema>> schema, Set<String> openTransactions)
    {
        super(schema, openTransactions);
    }

    @Override
    protected void handle(HttpExchange he)
            throws Exception
    {
        URI requestURI = he.getRequestURI();
        String query = requestURI.getQuery();
        LOG.info(format("GET %s", requestURI));
        ParsedURL parsedURL = ParsedURL.of(requestURI.getPath());
        if (parsedURL.isBaseUrl()) {
            respond(new MockListBucketsReply().apply(schemaMap), he, 200);
        }
        else if (parsedURL.isBucketURL()) {
            String bucket = parsedURL.getBucket();
            if (schemaMap.containsKey(bucket)) {
                Set<MockMapSchema> schemas = schemaMap.get(bucket);
                if (Objects.isNull(schemas) || schemas.isEmpty()) {
                    LOG.info("Bucket %s exists no schemas", bucket);
                    replyEmptySchemaList(he);
                }
                else {
                    LOG.info("Responding schemas for bucket %s", bucket);
                    Set<MockMapSchema> mockMapSchemas = schemaMap.get(bucket);
                    FlatBufferBuilder b = MockSchemaUtil.getListSchemasReply(bucket, mockMapSchemas);
                    respondFlatBuffer(he, b);
                }
            }
            else {
                LOG.info("Bucket %s does not exist", bucket);
                respondError(he);
            }
        }
        else if (parsedURL.isSchemaURL()) {
            String bucket = parsedURL.getBucket();
            if (!schemaMap.containsKey(bucket)) {
                respond(format("Bucket %s does not exist", bucket), he, 404);
            }
            String schemaName = parsedURL.getSchemaName();
            Headers requestHeaders = he.getRequestHeaders();
            LOG.info("Responding tables for schema %s, query=%s, headers = %s", schemaName, query,
                    requestHeaders.entrySet());
            List<String> exactMatchHeaderValue = requestHeaders.get("tabular-name-exact-match");
            String exactMatch = (exactMatchHeaderValue == null || exactMatchHeaderValue.isEmpty()) ? null : Iterables.getOnlyElement(exactMatchHeaderValue);
            Set<MockMapSchema> mockMapSchemas = schemaMap.get(bucket);
            Optional<MockMapSchema> existingSchema = mockMapSchemas.stream().filter(mockSchema -> mockSchema.getName().equals(schemaName)).findAny();
            switch (query) {
                case "table":
                case "view":
                    if (existingSchema.isPresent()) {
                        MockMapSchema mockMapSchema = existingSchema.get();
                        LOG.info("mockMapSchema = %s", mockMapSchema);
                        FlatBufferBuilder b;
                        if (query.equals("table")) {
                            b = MockSchemaUtil.getListTablesReply(bucket, schemaName, mockMapSchema, exactMatch);
                        }
                        else {
                            b = MockSchemaUtil.getListViewsReply(bucket, schemaName, mockMapSchema, exactMatch);
                        }
                        respondFlatBuffer(he, b);
                    }
                    else {
                        respond(format("Schema %s does not exist in bucket %s", schemaName, bucket), he, 404);
                    }
                    break;
                case "schema":
                    // probably a nested schema url reply empty schema list
                    replyEmptySchemaList(he);
                    break;
            }
        }
        else if (parsedURL.hasTable()) {
            if (query.equals("schedule")) {
                respondSchedulingInfo(he);
            }
            else {
                String bucket = parsedURL.getBucket();
                if (!schemaMap.containsKey(bucket)) {
                    respond(format("Bucket %s does not exist", bucket), he, 404);
                }
                String schemaName = parsedURL.getSchemaName();
                String tableName = parsedURL.getTableName();
                Set<MockMapSchema> mockMapSchemas = schemaMap.get(bucket);
                Optional<MockMapSchema> existingSchema = mockMapSchemas.stream().filter(mockSchema -> mockSchema.getName().equals(schemaName)).findAny();
                if (existingSchema.isPresent()) {
                    if (query.equals("data")) {
                        MockTable mockTable = existingSchema.get().getTables().get(tableName);
                        if (mockTable == null) {
                            respond(format("Table %s does not exist in schema %s/%s", tableName, schemaName, bucket), he, 404);
                        }
                        else {
                            byte[] bytes = readAllBytes(he.getRequestBody());
                            LOG.info("QUERY_DATA: %s", new String(bytes, Charset.defaultCharset())); //TODO - validate schema
                            respondError(he);
                        }
                    }
                    else {
                        Map<String, MockTable> tables = existingSchema.get().getTables();
                        LOG.info("Responding columns for table %s, Existing tables: %s, request = %s", tableName, tables, he.getResponseHeaders());
                        if (tables.containsKey(tableName)) {
                            MockTable mockTable = tables.get(tableName);
                            Collection<Field> values = mockTable.getColumns().values();
                            Optional<byte[]> serializeFields = MockSchemaUtil.serializeFields(values);
                            if (serializeFields.isPresent()) {
                                respond(serializeFields.get(), he, 200);
                            }
                            else {
                                respondError(String.format("Failed serializing fields for table %s", tableName), he);
                            }
                        }
                        else {
                            respond(format("Schema %s does not exist in schema %s", tableName, parsedURL.getFullSchemaPath()), he, 404);
                        }
                    }
                }
            }
        }
        else {
            LOG.info("Not a valid parsedURL");
            respondError(he);
        }
    }

    private void replyEmptySchemaList(HttpExchange he)
            throws IOException
    {
        FlatBufferBuilder b = new FlatBufferBuilder();
        ListSchemasResponse.startListSchemasResponse(b);
        b.finish(ListSchemasResponse.endListSchemasResponse(b));
        respondFlatBuffer(he, b);
    }

    private void respondSchedulingInfo(HttpExchange he)
            throws IOException
    {
        he.getResponseHeaders().add("tabular-schedule-id", "12345678");
        respond("", he, 200);
    }

    private void respondFlatBuffer(HttpExchange he, FlatBufferBuilder b)
            throws IOException
    {
        ByteBuffer byteBuffer = b.dataBuffer();
        Consumer<OutputStream> osc = os -> {
            byte[] br = new byte[byteBuffer.remaining()];
            byteBuffer.get(br);
            try {
                os.write(br);
            }
            catch (Exception e) {
                throw new RuntimeException("Failed writing arrow stream", e);
            }
        };
        respond(osc, he);
    }
}
