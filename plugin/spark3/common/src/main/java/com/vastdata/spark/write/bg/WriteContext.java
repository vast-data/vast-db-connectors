/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.tx.VastTransaction;

import java.net.URI;
import java.util.function.Function;

public final class WriteContext
{
    private final Function<VastConfig, VastClient> vastClientSupplier;
    private final String dataWriteTraceToken;
    private final VastConfig vastConfig;
    private final URI endpoint;
    private final VastTransaction tx;
    private final String schemaName;
    private final String tableName;
    private final String endUser;

    public WriteContext(
            Function<VastConfig, VastClient> vastClientSupplier,
            String dataWriteTraceToken,
            VastConfig vastConfig,
            URI endpoint,
            VastTransaction tx,
            String schemaName,
            String tableName,
            String endUser)
    {
        this.vastClientSupplier = vastClientSupplier;
        this.dataWriteTraceToken = dataWriteTraceToken;
        this.vastConfig = vastConfig;
        this.endpoint = endpoint;
        this.tx = tx;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.endUser = endUser;
    }

    public Function<VastConfig, VastClient> getVastClientSupplier() { return vastClientSupplier; }
    public String getDataWriteTraceToken() { return dataWriteTraceToken; }
    public VastConfig getVastConfig() { return vastConfig; }
    public URI getEndpoint() { return endpoint; }
    public VastTransaction getTx() { return tx; }
    public String getSchemaName() { return schemaName; }
    public String getTableName() { return tableName; }
    public String getEndUser() { return endUser; }
}

