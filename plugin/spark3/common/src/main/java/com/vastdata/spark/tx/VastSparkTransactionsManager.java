/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.tx;

import com.vastdata.client.VastClient;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTransactionFactory;
import com.vastdata.client.tx.VastTransactionHandleManager;

import java.util.Optional;

public class VastSparkTransactionsManager
        extends VastTransactionHandleManager<SimpleVastTransaction>
{
    private static VastSparkTransactionsManager instance = null;

    private VastSparkTransactionsManager(VastClient client,
            VastTransactionFactory transactionInstantiationFunction)
    {
        super(client, Optional.empty(), transactionInstantiationFunction);
    }

    public static VastSparkTransactionsManager getInstance(VastClient client,
            VastTransactionFactory transactionInstantiationFunction)
    {
        if (instance == null) {
            initInstance(client, transactionInstantiationFunction);
        }
        return instance;
    }

    private static synchronized void initInstance(VastClient client,
            VastTransactionFactory transactionInstantiationFunction)
    {
        if (instance == null) {
            instance = new VastSparkTransactionsManager(client,
                    transactionInstantiationFunction);
        }
    }
}
