/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx;

import com.vastdata.client.VastClient;
import com.vastdata.client.error.ErrorType;
import com.vastdata.client.error.VastIOException;
import com.vastdata.client.error.VastRuntimeException;
import io.airlift.log.Logger;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public class VastAutocommitTransaction implements VastTransaction, AutoCloseable, Serializable
{
    private static final Logger LOG = Logger.get(VastAutocommitTransaction.class);
    private final String endUser;
    private final VastClient client;
    private final VastTransaction transaction;
    private final boolean autoCreated;
    private boolean rollback = false;

    public static BiConsumer<Boolean, UnaryOperator<Optional<String>>> alterTransaction = (cancelOnFailure, f) -> {
        throw new IllegalStateException("Env supplier is unset");
    };

    private VastAutocommitTransaction(VastClient client, VastTransaction transaction, boolean autoCreated, final String endUser) {
        this.client = client;
        if (transaction == null) {
            throw new RuntimeException("missing transaction");
        }
        this.transaction = transaction;
        this.autoCreated = autoCreated;
        this.endUser = endUser;
    }

    public VastAutocommitTransaction(SimpleVastTransaction fromString, boolean autoCreated, final String endUser) {
        this.client = null;
        this.transaction = fromString;
        this.autoCreated = autoCreated;
        this.endUser = endUser;
    }

    @Override
    public void close()
    {
        if (!autoCreated) {
            LOG.debug("VastAutocommitTransaction.wrap CLOSE explicit tx: tx: {}", transaction);
            // manually created, therefore should be manually closed
            return;
        }
        if (client != null) {
            if (rollback) {
                LOG.debug("VastAutocommitTransaction.wrap ROLLBACK: tx: {}", transaction);
                client.rollbackTransaction(transaction, endUser);
            }
            else {
                LOG.debug("VastAutocommitTransaction.wrap COMMIT: tx: {}", transaction);
                client.commitTransaction(transaction, endUser);
            }
        }
        else {
            LOG.warn("VastAutocommitTransaction.wrap CLOSE autocommit without client: tx: {}", transaction);
        }
    }

    public VastTransaction getTransaction()
    {
        return transaction;
    }

    @Override
    public long getId()
    {
        return transaction.getId();
    }


    @Override
    public VastTraceToken generateTraceToken(Optional<String> userTraceToken)
    {
        return transaction.generateTraceToken(userTraceToken);
    }

    public static SimpleVastTransaction getExisting()
    {
        final AtomicReference<SimpleVastTransaction> result = new AtomicReference<>();
        alterTransaction.accept(false, maybeTransaction -> {
            if (maybeTransaction.isPresent()) {
                String tx = maybeTransaction.get();
                LOG.info("VastAutocommitTransaction.wrap EXISTING: tx: {}", tx);
                try {
                    result.set(SimpleVastTransaction.fromString(tx));
                }
                catch (final Exception error) {
                    if (error instanceof RuntimeException) {
                        throw (RuntimeException) error;
                    }
                    throw new VastRuntimeException("Failed getting existing transaction", error, ErrorType.GENERAL);
                }
            }
            else {
                LOG.debug("VastAutocommitTransaction.wrap EXISTING: null");
            }
            return maybeTransaction;
        });
        return result.get();
    }

    public static VastAutocommitTransaction wrapVastTransactionOrCreateNew(Optional<VastTransaction> tx, VastClient vastClient, Supplier<VastTransaction> vastTransactionSupplier, final String endUser) {
        if (tx != null && tx.isPresent()) {
            return new VastAutocommitTransaction(vastClient, tx.get(), false, endUser);
        }
        else {
            return createNewOrReuseFromEnv(vastClient, vastTransactionSupplier, endUser);
        }
    }

    public static VastAutocommitTransaction createNewOrReuseFromEnv(VastClient vastClient, Supplier<VastTransaction> vastTransactionSupplier, final String endUser) {
        final AtomicReference<VastAutocommitTransaction> result = new AtomicReference<>();
        alterTransaction.accept(false, maybeTransaction -> {
            if (maybeTransaction.isPresent()) {
                String tx = maybeTransaction.get();
                try {
                    LOG.info("VastAutocommitTransaction.wrap REUSE: tx: {}", tx);
                    result.set(new VastAutocommitTransaction(SimpleVastTransaction.fromString(tx), false, endUser));
                }
                catch (VastIOException e) {
                    throw new RuntimeException(e);
                }
            }
            else {
                VastAutocommitTransaction vastAutocommitTransaction = new VastAutocommitTransaction(vastClient, vastTransactionSupplier.get(), true, endUser);
                LOG.info("VastAutocommitTransaction.wrap NEW: {}", vastAutocommitTransaction);
                result.set(vastAutocommitTransaction);
            }
            return maybeTransaction;
        });
        return result.get();
    }

    public void setCommit(boolean mode)
    {
        this.rollback = !mode;
    }
}
