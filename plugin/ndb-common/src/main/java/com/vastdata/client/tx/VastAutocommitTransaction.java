/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx;

import com.vastdata.client.error.ErrorType;
import com.vastdata.client.error.VastIOException;
import com.vastdata.client.error.VastRuntimeException;
import io.airlift.log.Logger;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public class VastAutocommitTransaction
        implements VastTransaction, AutoCloseable, Serializable
{
    private static final Logger LOG = Logger.get(
            VastAutocommitTransaction.class);
    public static BiConsumer<Boolean, UnaryOperator<Optional<String>>> alterTransaction = (cancelOnFailure, f) -> {
        throw new IllegalStateException("Env supplier is unset");
    };
    private final String endUser;
    private final VastTransaction transaction;
    private final boolean autoCreated;
    private boolean rollback;
    private VastTransactionHandleManager<? extends VastTransaction> manager;

    private VastAutocommitTransaction(VastTransactionHandleManager<?> manager,
            VastTransaction transaction, boolean autoCreated,
            final String endUser)
    {
        this.manager = manager;
        if (transaction == null) {
            throw new RuntimeException("missing transaction");
        }
        this.transaction = transaction;
        this.autoCreated = autoCreated;
        this.endUser = endUser;
    }

    public VastAutocommitTransaction(VastTransaction fromString,
            boolean autoCreated, final String endUser)
    {
        this.transaction = fromString;
        this.autoCreated = autoCreated;
        this.endUser = endUser;
    }

    public static SimpleVastTransaction getExisting()
    {
        final AtomicReference<SimpleVastTransaction> result = new AtomicReference<>();
        alterTransaction.accept(false, maybeTransaction -> {
            if (maybeTransaction.isPresent()) {
                String tx = maybeTransaction.get();
                LOG.info("VastAutocommitTransaction.wrap EXISTING: tx: %s", tx);
                try {
                    result.set(SimpleVastTransaction.fromString(tx));
                }
                catch (final Exception error) {
                    if (error instanceof RuntimeException) {
                        throw (RuntimeException) error;
                    }
                    throw new VastRuntimeException(
                            "Failed getting existing transaction", error,
                            ErrorType.GENERAL);
                }
            }
            else {
                LOG.debug("VastAutocommitTransaction.wrap EXISTING: null");
            }
            return maybeTransaction;
        });
        return result.get();
    }

    public static VastAutocommitTransaction wrapVastTransactionOrCreateNew(
            Optional<VastTransaction> tx,
            VastTransactionHandleManager<?> manager,
            Supplier<VastTransaction> vastTransactionSupplier,
            final String endUser)
    {
        if (tx != null && tx.isPresent()) {
            return new VastAutocommitTransaction(manager, tx.get(), false,
                    endUser);
        }
        else {
            return createNewOrReuseFromEnv(manager, vastTransactionSupplier,
                    endUser);
        }
    }

    public static VastAutocommitTransaction createNewOrReuseFromEnv(
            VastTransactionHandleManager<?> manager,
            Supplier<VastTransaction> vastTransactionSupplier,
            final String endUser)
    {
        final AtomicReference<VastAutocommitTransaction> result = new AtomicReference<>();
        alterTransaction.accept(false, maybeTransaction -> {
            if (maybeTransaction.isPresent()) {
                String tx = maybeTransaction.get();
                try {
                    LOG.info("VastAutocommitTransaction.wrap REUSE: tx: %s",
                            tx);
                    result.set(new VastAutocommitTransaction(
                            SimpleVastTransaction.fromString(tx), false,
                            endUser));
                }
                catch (VastIOException e) {
                    throw new RuntimeException(e);
                }
            }
            else {
                VastAutocommitTransaction vastAutocommitTransaction = new VastAutocommitTransaction(
                        manager, vastTransactionSupplier.get(), true, endUser);
                LOG.info("VastAutocommitTransaction.wrap NEW: %s",
                        vastAutocommitTransaction);
                result.set(vastAutocommitTransaction);
            }
            return maybeTransaction;
        });
        return result.get();
    }

    @Override
    public void close()
    {
        if (!autoCreated) {
            LOG.debug(
                    "VastAutocommitTransaction.wrap CLOSE explicit tx: tx: %s",
                    transaction);
            // manually created, therefore should be manually closed
            return;
        }
        if (manager != null) {
            if (rollback) {
                LOG.debug("VastAutocommitTransaction.wrap ROLLBACK: tx: %s",
                        transaction);
                manager.rollback(transaction, endUser);
            }
            else {
                LOG.debug("VastAutocommitTransaction.wrap COMMIT: tx: %s",
                        transaction);
                manager.commit(transaction, endUser);
            }
        }
        else {
            LOG.warn(
                    "VastAutocommitTransaction.wrap CLOSE autocommit without client: tx: %s",
                    transaction);
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

    public void setCommit(boolean mode)
    {
        this.rollback = !mode;
    }

    public <T> T executeWithRollbackOnFailure(Callable<T> action)
            throws Exception
    {
        try {
            return action.call();
        }
        catch (Throwable t) {
            this.setCommit(false);
            throw t;
        }
    }
}
