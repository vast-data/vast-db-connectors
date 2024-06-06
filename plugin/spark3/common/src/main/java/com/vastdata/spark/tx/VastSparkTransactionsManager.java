/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.tx;

import com.vastdata.client.VastClient;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTransactionHandleManager;

public class VastSparkTransactionsManager extends VastTransactionHandleManager<SimpleVastTransaction>
{
    private static VastSparkTransactionsManager instance = null;

    private VastSparkTransactionsManager(VastClient client, VastSimpleTransactionFactory transactionInstantiationFunction)
    {
        super(client, transactionInstantiationFunction);
    }

    public static VastSparkTransactionsManager getInstance(VastClient client, VastSimpleTransactionFactory transactionInstantiationFunction)
    {
        if (instance == null) {
            initInstance(client, transactionInstantiationFunction);
        }
        return instance;
    }

    private static synchronized void initInstance(VastClient client, VastSimpleTransactionFactory transactionInstantiationFunction)
    {
        if (instance == null) {
            instance = new VastSparkTransactionsManager(client, transactionInstantiationFunction);
        }
    }
}
