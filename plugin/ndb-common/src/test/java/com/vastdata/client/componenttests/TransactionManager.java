/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.componenttests;

import com.vastdata.client.VastClient;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTransactionFactory;
import com.vastdata.client.tx.VastTransactionHandleManager;

import java.util.Optional;

public class TransactionManager
        extends VastTransactionHandleManager<SimpleVastTransaction>
{
    private static TransactionManager instance;

    TransactionManager(VastClient client,
            VastTransactionFactory transactionInstantiationFunction)
    {
        super(client, Optional.empty(), transactionInstantiationFunction);
    }
}
