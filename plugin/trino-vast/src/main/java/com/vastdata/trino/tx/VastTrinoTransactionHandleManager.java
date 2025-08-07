/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.tx;

import com.google.inject.Inject;
import com.vastdata.client.VastClient;
import com.vastdata.client.tx.VastAutocommitTransaction;
import com.vastdata.client.tx.VastTransactionHandleManager;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

public class VastTrinoTransactionHandleManager
        extends VastTransactionHandleManager<VastTransactionHandle>
{
    public static final BiConsumer<Boolean, UnaryOperator<Optional<String>>> ALWAYS_EMPTY_TRANSACTION = (_, f) -> f.apply(Optional.empty());

    static {
        VastAutocommitTransaction.alterTransaction = ALWAYS_EMPTY_TRANSACTION;
    }

    @Inject
    public VastTrinoTransactionHandleManager(VastClient client, VastTransactionHandleFactory transactionInstantiationFunction)
    {
        super(client, transactionInstantiationFunction);
    }
}
