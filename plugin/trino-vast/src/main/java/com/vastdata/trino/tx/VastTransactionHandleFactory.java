/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.tx;

import com.vastdata.client.tx.VastTransaction;
import com.vastdata.client.tx.VastTransactionInstantiator;

public class VastTransactionHandleFactory
        implements VastTransactionInstantiator<VastTransactionHandle>
{
    @Override
    public VastTransactionHandle apply(VastTransaction tx)
    {
        return new VastTransactionHandle(tx.getId());
    }
}
