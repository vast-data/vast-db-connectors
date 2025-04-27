/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver.handle;

import com.google.common.collect.Iterables;
import com.sun.net.httpserver.HttpExchange;
import com.vastdata.client.ParsedURL;
import com.vastdata.mockserver.MockMapSchema;
import com.vastdata.mockserver.MockTable;
import com.vastdata.mockserver.MockView;
import io.airlift.log.Logger;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class DeleteHandler
        extends AbstractRequestConsumer
{
    private static final Logger LOG = Logger.get(DeleteHandler.class);

    public DeleteHandler(Map<String, Set<MockMapSchema>> schema, Set<String> openTransactions)
    {
        super(schema, openTransactions);
    }

    @Override
    protected void handle(HttpExchange he)
            throws Exception
    {
        URI requestURI = he.getRequestURI();
        String query = requestURI.getQuery();
        LOG.info(format("DELETE %s, %s", requestURI, query));
        ParsedURL parsedURL = ParsedURL.of(requestURI.getPath());
        if (parsedURL.isBaseUrl()) {
            removeTransaction(he);
        }
        else if (parsedURL.isBucketURL()) {
            String bucket = parsedURL.getBucket();
            if (!schemaMap.containsKey(bucket)) {
                LOG.error("Bucket %s does not exist", bucket);
                respondError(he);
            }
            else {
                LOG.info("Bucket %s exists, deleting", bucket);
                schemaMap.remove(bucket);
                respond("", he, 200);
            }
        }
        else if (parsedURL.isSchemaURL()) {
            String bucket = parsedURL.getBucket();
            if (!schemaMap.containsKey(bucket)) {
                LOG.error("Bucket %s does not exist", bucket);
                respondError(he);
            }
            else {
                LOG.info("Bucket %s exists, deleting", bucket);
                Set<MockMapSchema> mockMapSchemas = schemaMap.get(bucket);
                String schemaName = parsedURL.getSchemaName();
                Optional<MockMapSchema> existingSchema = mockMapSchemas.stream().filter(mockSchema -> mockSchema.getName().equals(schemaName)).findAny();
                if (existingSchema.isPresent()) {
                    Set<MockMapSchema> filteredSchemas = mockMapSchemas.stream().filter(mockSchema -> !mockSchema.getName().equals(schemaName)).collect(Collectors.toSet());
                    schemaMap.put(bucket, filteredSchemas);
                    respond("", he, 200);
                }
                else {
                    LOG.error("Schema %s does not exist", schemaName);
                    respondError(he);
                }
            }
        }
        else if (parsedURL.hasTable()) {
            String bucket = parsedURL.getBucket();
            if (!schemaMap.containsKey(bucket)) {
                LOG.error("Bucket %s does not exist", bucket);
                respondError(he);
            }
            else {
                Set<MockMapSchema> mockMapSchemas = schemaMap.get(bucket);
                String schemaName = parsedURL.getSchemaName();
                Optional<MockMapSchema> existingSchema = mockMapSchemas.stream().filter(mockSchema -> mockSchema.getName().equals(schemaName)).findAny();
                if (existingSchema.isPresent()) {
                    String tableName = parsedURL.getTableName();
                    if (query.equals("view")) {
                        Map<String, MockView> views = existingSchema.get().getViews();
                        if (!views.containsKey(tableName)) {
                            LOG.error("View %s does not exist", tableName);
                            respondError(he);
                        }
                        else {
                            if (views.remove(tableName) == null) {
                                LOG.error("Failed deleting view %s", tableName);
                                respondError(he);
                            }
                            else {
                                respond("", he, 200);
                                return;
                            }
                        }
                    }
                    Map<String, MockTable> tables = existingSchema.get().getTables();
                    if (!tables.containsKey(tableName)) {
                        LOG.error("Table %s does not exist", tableName);
                        respondError(he);
                    }
                    else {
                        switch (query) {
                            case "table":
                                if (tables.remove(tableName) == null) {
                                    LOG.error("Failed deleting table %s", tableName);
                                    respondError(he);
                                }
                                else {
                                    respond("", he, 200);
                                }
                                break;
                            case "column":
                                MockTable mockTable = tables.get(tableName);
                                Map<String, Field> columns = mockTable.getColumns();
                                String colName = readColumnSchema(he);
                                if (columns.remove(colName) == null) {
                                    LOG.error("Column %s does not exist in table %s", colName, tableName);
                                    respondError(he);
                                }
                                else {
                                    respond("", he, 200);
                                }
                                break;
                            default:
                                respondError(he);
                        }

                    }
                }
                else {
                    LOG.error("Schema %s does not exist", schemaName);
                    respondError(he);
                }
            }
        }
        else {
            respondError(he);
        }
    }

    private static String readColumnSchema(HttpExchange he)
            throws IOException
    {
        try (ArrowStreamReader reader = new ArrowStreamReader(he.getRequestBody(), new RootAllocator())) {
            reader.loadNextBatch();
            Schema schema = reader.getVectorSchemaRoot().getSchema();
            List<Field> fields = schema.getFields();
            return Iterables.getOnlyElement(fields).getName();
        }
    }
}
