/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.tx;

import com.vastdata.client.VastClient;
import com.vastdata.client.tx.VastTransactionHandleManager;

import javax.inject.Inject;

public class VastTrinoTransactionHandleManager
        extends VastTransactionHandleManager<VastTransactionHandle>
{
    @Inject
    public VastTrinoTransactionHandleManager(VastClient client, VastTransactionHandleFactory transactionInstantiationFunction)
    {
        super(client, transactionInstantiationFunction);
    }
}
