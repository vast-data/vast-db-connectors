/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx;

public class VastTransactionFactory
        implements VastTransactionInstantiator<SimpleVastTransaction>
{
    @Override
    public SimpleVastTransaction apply(VastTransaction tx)
    {
        return new SimpleVastTransaction(tx.getId());
    }
}
