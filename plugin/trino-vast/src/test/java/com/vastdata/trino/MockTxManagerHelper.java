/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.trino.tx.VastTransactionHandle;
import com.vastdata.trino.tx.VastTrinoTransactionHandleManager;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class MockTxManagerHelper
{
    private final VastTrinoTransactionHandleManager txManager;

    private MockTxManagerHelper(VastTrinoTransactionHandleManager transactionHandleManager)
    {
        this.txManager = transactionHandleManager;
    }

    public static MockTxManagerHelper forTxManager(VastTrinoTransactionHandleManager transactionHandleManager)
    {
        return new MockTxManagerHelper(transactionHandleManager);
    }

    public MockTxManagerHelper registerTx(long id)
    {
        when(txManager.isOpen(any(VastTransactionHandle.class))).thenReturn(true);
        when(txManager.startTransaction(any())).thenReturn(new VastTransactionHandle(id));
        return this;
    }
}
