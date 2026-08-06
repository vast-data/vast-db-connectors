/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.bycolumninserter;

import com.vastdata.client.QueryDataExtraParams;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastException;
import com.vastdata.client.metrics.ByColumnInserterMetrics;
import com.vastdata.client.metrics.TimeMeasure;
import com.vastdata.client.rowid.RowIDStrategyType;
import com.vastdata.client.tx.VastTransaction;
import io.airlift.log.Logger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecuteFuncs
{
    private static final Logger LOG = Logger.get(ByColumnInserter.class);
    private static final AtomicInteger endpointUseCounter = new AtomicInteger();
    private final VastClient vastClient;
    private final String path;
    private final VastTransaction transaction;
    private final List<URI> dataEndpoints;
    private final QueryDataExtraParams extraQueryParams;
    private final String endUser;
    private final ByColumnInserterMetrics metrics;
    private final String traceToken;

    public ExecuteFuncs(VastClient vastClient, VastConfig vastConfig,
            RowIDStrategyType rowIdType, String path,
            VastTransaction transaction, List<URI> dataEndpoints,
                        QueryDataExtraParams extraQueryParams,
                        String endUser, ByColumnInserterMetrics metrics, String traceToken)
    {
        this.vastClient = vastClient;

        this.transaction = transaction;
        this.extraQueryParams = extraQueryParams;
        this.endUser = endUser;
        this.dataEndpoints = dataEndpoints;
        this.path = path;
        this.metrics = metrics;
        this.traceToken = traceToken;
    }

    private URI getNextEndpointUri()
    {
        int counter = endpointUseCounter.getAndIncrement();
        return dataEndpoints.get(counter % dataEndpoints.size());
    }

    public void update(byte[] payload)
            throws VastException
    {
        LOG.warn("%s before update", traceToken);
        TimeMeasure timing = TimeMeasure.createAndStart();
        vastClient.updateRows(transaction, path, payload, getNextEndpointUri(), extraQueryParams, endUser);
        timing.end(x -> {
            metrics.recordUpdateTime(x);
            LOG.warn("%s after update: nanos: %f", traceToken, x / 1000000000.0);
        });
    }

    public VectorSchemaRoot insert(byte[] payload,
            BufferAllocator resultAllocator)
            throws VastException
    {
        TimeMeasure timing = TimeMeasure.createAndStart();
        LOG.debug("%s before insert", traceToken);
        VectorSchemaRoot rowIds = vastClient.insertRows(transaction,
                getNextEndpointUri(), path, payload, true, extraQueryParams,
                endUser,
                resultAllocator);

        timing.end(x -> {
            metrics.recordInsertTime(x);
            LOG.debug("%s after insert: nanos: %f", traceToken, x / 1000000000.0);
        });
        return rowIds;
    }
}
