/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.VastClient;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

public class VastPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final VastClient client;

    @Inject
    public VastPageSinkProvider(VastClient client)
    {
        this.client = client;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        return new VastPageSink(client, session, (VastTransactionHandle) transactionHandle, (VastInsertTableHandle) outputTableHandle);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        return new VastPageSink(client, session, (VastTransactionHandle) transactionHandle, (VastInsertTableHandle) insertTableHandle);
    }
}
