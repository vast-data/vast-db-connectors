/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.error;

import com.vastdata.client.tx.VastTransaction;

public class VastClosedTransactionException
        extends VastRuntimeException
{
    VastClosedTransactionException(VastTransaction transactionHandle)
    {
        super(String.format("Transaction %s is closed", transactionHandle), ErrorType.CLIENT);
    }
}
