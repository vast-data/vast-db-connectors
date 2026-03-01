/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.vdb.sdk;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastUserException;
import io.airlift.http.client.HttpClient;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class VastSdk
{
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_SLEEP_DURATION_MILLIS = 10;

    private final Map<String, Schema> tableCache = new ConcurrentHashMap<>();

    private final VastConfig config;
    private final VastClient client;
    private final RetryConfig retryConfig;

    public VastSdk(HttpClient httpClient, VastSdkConfig config)
    {
        this(httpClient, config, new RetryConfig(DEFAULT_MAX_RETRIES, DEFAULT_SLEEP_DURATION_MILLIS));
    }

    public VastSdk(HttpClient httpClient, VastSdkConfig config, RetryConfig retryConfig)
    {
        this.config = requireNonNull(config).getVastConfig();
        this.client =  new VastClient(requireNonNull(httpClient), config.getVastConfig(), new VastSdkDependenciesFactory(config.getVastConfig()));
        this.retryConfig = requireNonNull(retryConfig);
    }

    public VastClient getVastClient()
    {
        return client;
    }

    public Table getTable(String schemaName, String tableName)
    {
        return new Table(schemaName, tableName, client, config.getDataEndpoints(), retryConfig);
    }

    public Iterator<VectorSchemaRoot> executeQuery(String statement)
            throws TableSchemaNotLoadedException, VastUserException
    {
        String schemaTable = CalciteSerializer.getTableName(statement);
        if (schemaTable == null || !schemaTable.contains(".")) {
            throw new VastUserException("Query must specify schema and table name in the format schema.table");
        }
        String schemaName = schemaTable.substring(0, schemaTable.lastIndexOf("."));
        String tableName = schemaTable.substring(schemaTable.lastIndexOf(".") + 1);
        Schema tableSchema = tableCache.computeIfAbsent(schemaTable, (k) -> buildSchema(client, schemaName, tableName));
        Table queryTable = new Table(statement, tableSchema, schemaName,  tableName, client, config.getDataEndpoints(), retryConfig);
        return new ResultIterator(queryTable);
    }

    /**
     * Flush table cache. If both schemaName and tableName are present, flush only the specific table.
     * Otherwise, flush the entire cache.
     * @param schemaName
     * @param tableName
     */
    public boolean flushTableCache(Optional<String> schemaName, Optional<String> tableName)
    {
        if (schemaName.isPresent() && tableName.isPresent()) {
            String schemaTable = schemaName.get() + "." + tableName.get();
            return tableCache.remove(schemaTable) != null;
        }
        else {
            tableCache.clear();
        }
        return true;
    }

    private static Schema buildSchema(VastClient client, String schemaName, String tableName)
            throws RuntimeException
    {
        List<Field> fields;
        try {
            fields = client.listColumns(null,
                    schemaName,
                    tableName,
                    1000,
                    Collections.emptyMap(),
                    null);
        }
        catch (VastException e) {
            throw new RuntimeException(e);
        }
        return new Schema(fields);
    }
}