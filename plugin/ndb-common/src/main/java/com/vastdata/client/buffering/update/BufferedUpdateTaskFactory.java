/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.buffering.update;

import com.vastdata.client.QueryDataExtraParams;
import com.vastdata.client.VastClient;
import com.vastdata.client.buffering.BufferedRowIdBasedWriteTaskFactory;
import com.vastdata.client.error.VastException;
import com.vastdata.client.tx.VastTransaction;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class BufferedUpdateTaskFactory
        extends BufferedRowIdBasedWriteTaskFactory
{
    private final VastClient client;
    private final VastTransaction tx;
    private final String schema;
    private final String table;
    private final QueryDataExtraParams extraQueryParams;
    private final String endUser;

    public BufferedUpdateTaskFactory(VastClient client, VastTransaction tx,
            String schema, String table,
                                     QueryDataExtraParams extraQueryParams,
                                     String endUser,
            List<URI> dataEndpoints, ExecutorService ioExecutor)
    {
        super(dataEndpoints, ioExecutor);

        this.client = client;
        this.tx = tx;
        this.schema = schema;
        this.table = table;
        this.extraQueryParams = extraQueryParams;
        this.endUser = endUser;
    }

    @Override
    protected void sendVsr(VectorSchemaRoot vsr, URI dataEndpoint,
            Optional<Integer> maxRowsPerRpc)
            throws VastException
    {
        client.updateRows(tx, schema, table, vsr, dataEndpoint, maxRowsPerRpc,
                extraQueryParams,
                endUser);
    }

    @Override
    public String getTaskName()
    {
        return "Update";
    }
}
