/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver.handle;

import com.sun.net.httpserver.HttpExchange;
import com.vastdata.client.ParsedURL;
import com.vastdata.mockserver.MockMapSchema;
import com.vastdata.mockserver.MockTable;
import io.airlift.log.Logger;

import java.net.URI;
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
        LOG.info(format("DELETE %s", requestURI));
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
                    Map<String, MockTable> tables = existingSchema.get().getTables();
                    if (tables.remove(tableName) == null) {
                        LOG.error("Table %s does not exist", tableName);
                        respondError(he);
                    }
                    else {
                        respond("", he, 200);
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
}
