/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx;

import com.google.common.collect.Sets;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastResponse;
import com.vastdata.client.error.VastExceptionFactory;
import com.vastdata.client.schema.StartTransactionContext;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.Set;

import static java.lang.String.format;

public abstract class VastTransactionHandleManager<T extends VastTransaction>
{
    private static final Logger LOG = Logger.get(VastTransactionHandleManager.class);

    private final VastClient client;
    private final VastTransactionResponseParser parser = new VastTransactionResponseParser();
    private final Set<T> openTransactions = Sets.newConcurrentHashSet();
    private final VastTransactionInstantiator<T> transactionInstantiationFunction;

    @Inject
    public VastTransactionHandleManager(VastClient client,
            VastTransactionInstantiator<T> transactionInstantiationFunction)
    {
        this.client = client;
        this.transactionInstantiationFunction = transactionInstantiationFunction;
    }

    public T startTransaction(StartTransactionContext ctx)
    {
        ParsedStartTransactionResponse parsedResponse = parser.apply(client.startTransaction(ctx));
        T newTransHandle = transactionInstantiationFunction.apply(ctx, parsedResponse);
        LOG.debug("Opened new transaction: %s", newTransHandle);
        openTransactions.add(newTransHandle);
        return newTransHandle;
    }

    public void commit(T handle)
    {
        if (!openTransactions.remove(handle)) {
            LOG.error("Committing not open transaction: %s", handle);
        }
        VastResponse response = client.commitTransaction(handle);
        VastExceptionFactory.checkResponseStatus(response, format("Commit %s failed: %s", handle, response)).ifPresent(exception -> {
            LOG.error(exception, "Failed committing transaction %s: %s", handle, response);
            throw VastExceptionFactory.toRuntime(VastExceptionFactory.serverException("Failed committing transaction", exception));
        });
    }

    public void rollback(T handle)
    {
        if (!openTransactions.remove(handle)) {
            LOG.error("Rolling back not open transaction: %s", handle);
        }
        VastResponse response = client.rollbackTransaction(handle);
        VastExceptionFactory.checkResponseStatus(response, format("Rollback %s failed: %s", handle, response)).ifPresent(exception -> {
            LOG.error(exception, "Failed rolling back transaction %s: %s", handle, response);
            throw VastExceptionFactory.toRuntime(VastExceptionFactory.serverException("Failed rolling back transaction", exception));
        });
    }

    public boolean isOpen(T transactionHandle)
    {
        return openTransactions.contains(transactionHandle);
    }
}
