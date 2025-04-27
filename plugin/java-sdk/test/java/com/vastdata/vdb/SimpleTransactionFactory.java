package com.vastdata.vdb;

import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.tx.ParsedStartTransactionResponse;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTransactionInstantiator;

public class SimpleTransactionFactory
        implements VastTransactionInstantiator<SimpleVastTransaction>
{
    @Override
    public SimpleVastTransaction apply(StartTransactionContext startTransactionContext, ParsedStartTransactionResponse parsedStartTransactionResponse)
    {
        return new SimpleVastTransaction(parsedStartTransactionResponse.getId(), startTransactionContext.isReadOnly(), startTransactionContext.isAutoCommit());
    }
}
