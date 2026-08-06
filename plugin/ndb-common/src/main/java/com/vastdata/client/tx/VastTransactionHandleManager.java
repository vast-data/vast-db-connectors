/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastResponse;
import com.vastdata.client.error.VastExceptionFactory;
import com.vastdata.client.queryengine.VastQueryEngineClient;
import io.airlift.log.Logger;
import vastdb.queryengine.protocol.QueryId;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;

public abstract class VastTransactionHandleManager<T extends VastTransaction>
{
    private static final Logger LOG = Logger.get(
            VastTransactionHandleManager.class);

    private final VastClient client;
    private final Optional<VastQueryEngineClient> queryEngineClient;
    private final Set<VastTransaction> openTransactions = Sets.newConcurrentHashSet();
    private final VastTransactionInstantiator<T> transactionInstantiationFunction;
    private final Multimap<VastTransaction, QueryId> queryEngineTxs = HashMultimap.create();

    public VastTransactionHandleManager(VastClient client,
            Optional<VastQueryEngineClient> queryEngineClient,
            VastTransactionInstantiator<T> transactionInstantiationFunction)
    {
        this.client = client;
        this.queryEngineClient = queryEngineClient;
        this.transactionInstantiationFunction = transactionInstantiationFunction;
    }

    public T startTransaction(final String endUser)
    {
        VastTransaction tx = client.startTransaction(endUser);
        T newTransHandle = transactionInstantiationFunction.apply(tx);
        LOG.debug("Opened new transaction: %s", newTransHandle);
        openTransactions.add(newTransHandle);
        return newTransHandle;
    }

    /**
     * we assume that the order of operations in a simple SELECT query: t0 start
     * query tx t1 start QE tx t2 finish QE tx t3 finish query TX
     *
     * Trino Transactional query flow: t0 start trino transaction t0 start QE tx
     * t1 SELECT with QE query t2 SELECT with QE query t3 SELECT with QE query
     * t4 commit/rollback trino transaction (while QE queries may be in-flight)
     * - we expect that in this case both the QE tx and QE queries will be
     * finished.
     */
    public void commit(VastTransaction handle, final String endUser)
    {
        if (!openTransactions.remove(handle)) {
            LOG.error("Committing not open transaction: %s", handle);
        }
        unregisterQEQueries(handle, "commit");
        VastResponse response = client.commitTransaction(handle, endUser);
        VastExceptionFactory
                .checkResponseStatus(response,
                        format("Commit %s failed: %s", handle, response))
                .ifPresent(exception -> {
                    LOG.error(exception, "Failed committing transaction %s: %s",
                            handle, response);
                    throw VastExceptionFactory.toRuntime(
                            VastExceptionFactory.serverException(
                                    "Failed committing transaction",
                                    exception));
                });
    }

    public void rollback(VastTransaction handle, final String endUser)
    {
        if (!openTransactions.remove(handle)) {
            LOG.error("Rolling back not open transaction: %s", handle);
        }
        unregisterQEQueries(handle, "rollback");
        VastResponse response = client.rollbackTransaction(handle, endUser);
        VastExceptionFactory
                .checkResponseStatus(response,
                        format("Rollback %s failed: %s", handle, response))
                .ifPresent(exception -> {
                    LOG.error(exception,
                            "Failed rolling back transaction %s: %s", handle,
                            response);
                    throw VastExceptionFactory.toRuntime(
                            VastExceptionFactory.serverException(
                                    "Failed rolling back transaction",
                                    exception));
                });
    }

    public boolean isOpen(T transactionHandle)
    {
        return openTransactions.contains(transactionHandle);
    }

    public void registerQEQuery(T transactionHandle, QueryId queryId)
    {
        queryEngineTxs.put(transactionHandle, queryId);
    }

    private void unregisterQEQueries(VastTransaction transactionHandle,
            String reason)
    {
        Collection<QueryId> queryIds = queryEngineTxs.get(transactionHandle);
        if (queryIds.isEmpty() || !queryEngineClient.isPresent()) {
            return;
        }
        queryIds.forEach((queryId -> {
            try {
                queryEngineClient.get().finishQuery(queryId, reason);
            }
            catch (Exception e) {
                LOG.warn(e, "failed to finish query %s for transaction %s: %s",
                        queryId, transactionHandle, reason);
            }
        }));
    }
}
