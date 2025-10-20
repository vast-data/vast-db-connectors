/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.tx;

import com.google.inject.Inject;
import com.vastdata.client.VastClient;
import com.vastdata.client.tx.VastTransactionHandleManager;

public class VastTrinoTransactionHandleManager
        extends VastTransactionHandleManager<VastTransactionHandle>
{
    @Inject
    public VastTrinoTransactionHandleManager(VastClient client, VastTransactionHandleFactory transactionInstantiationFunction)
    {
        super(client, transactionInstantiationFunction);
    }
}
