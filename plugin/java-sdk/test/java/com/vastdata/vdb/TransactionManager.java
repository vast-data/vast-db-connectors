package com.vastdata.vdb;

import com.vastdata.client.VastClient;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTransactionHandleManager;

public class TransactionManager
        extends VastTransactionHandleManager<SimpleVastTransaction>
{
    private static TransactionManager instance;

    TransactionManager(VastClient client, SimpleTransactionFactory transactionInstantiationFunction)
    {
        super(client, transactionInstantiationFunction);
    }

    public static TransactionManager getInstance(VastClient client, SimpleTransactionFactory transactionInstantiationFunction)
    {
        if (instance == null) {
            initInstance(client, transactionInstantiationFunction);
        }
        return instance;
    }

    private static synchronized void initInstance(VastClient client, SimpleTransactionFactory transactionInstantiationFunction)
    {
        if (instance == null) {
            instance = new TransactionManager(client, transactionInstantiationFunction);
        }
    }
}
