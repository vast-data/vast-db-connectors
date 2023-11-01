/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx.ka;

import com.vastdata.client.VastClient;
import com.vastdata.client.tx.VastTransaction;
import io.airlift.log.Logger;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class KeepAliveRequestExecutor implements Consumer<VastTransaction>
{
    private static final Logger LOG = Logger.get(KeepAliveRequestExecutor.class);

    private final Supplier<VastClient> vastClientSupplier;

    public KeepAliveRequestExecutor(Supplier<VastClient> vastClientSupplier) {
        this.vastClientSupplier = vastClientSupplier;
    }

    @Override
    public void accept(VastTransaction tx)
    {
        LOG.info("Executing keepalive for tx: %s", tx);
        try {
            vastClientSupplier.get().transactionKeepAlive(tx);
        }
        catch (Exception any) {
            LOG.error(any, "Transaction keepalive failed for tx: %s", tx);
        }
    }
}
