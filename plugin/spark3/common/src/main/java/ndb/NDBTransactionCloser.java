/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.client.tx.VastTransactionHandleManager;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class NDBTransactionCloser
        implements Consumer<VastTransaction>
{
    private VastTransactionHandleManager<SimpleVastTransaction> txManager = null;
    private final Supplier<VastTransactionHandleManager<SimpleVastTransaction>> txManagerSupplier;

    NDBTransactionCloser(Supplier<VastTransactionHandleManager<SimpleVastTransaction>> txManagerSupplier)
    {
        this.txManagerSupplier = txManagerSupplier;
    }

    @Override
    public void accept(VastTransaction vastTransaction)
    {
        getTxManager().commit((SimpleVastTransaction) vastTransaction, null);
    }

    private VastTransactionHandleManager<SimpleVastTransaction> getTxManager()
    {
        if (txManager == null)
        {
            initTxManager();
        }
        return txManager;
    }

    private synchronized void initTxManager()
    {
        if (txManager == null)
        {
            txManager = txManagerSupplier.get();
        }
    }
}
