/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx.ka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.tx.VastTransaction;
import io.airlift.log.Logger;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class JobEventService implements Runnable
{
    private static final Logger LOG = Logger.get(JobEventService.class);

    private static final BiFunction<VastTransaction, AtomicInteger, AtomicInteger> REF_COUNT_INCREASE = (key, currValue) -> {
        if (currValue == null) {
            LOG.info("Initializing for tx: %s", key);
            currValue = new AtomicInteger(1);
        }
        else {
            currValue.incrementAndGet();
            LOG.info("Incrementing for tx: %s: %s", key, currValue);
        }
        return currValue;
    };
    private static final BiFunction<VastTransaction, AtomicInteger, AtomicInteger> REF_COUNT_DECREASE = (key, currValue) -> {
        currValue.decrementAndGet(); // nulls not allowed, used with computeIfPresent
        LOG.info("Decrementing for tx: %s: %s", key, currValue);
        return currValue;
    };

    private static final ScheduledExecutorService daemon;

    static {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("JobEventService").build();
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, threadFactory);
        executor.setRemoveOnCancelPolicy(true);
        daemon = executor;
    }

    private static JobEventService instance = null;

    public static synchronized JobEventService createInstance(Supplier<VastClient> vastClientSupplier, VastConfig vastConf)
    {
        if (instance == null) {
            instance = new JobEventService(vastConf, new KeepAliveRequestExecutor(vastClientSupplier));
        }
        return instance;
    }

    public static Optional<JobEventService> getInstance()
    {
        return Optional.ofNullable(instance);
    }

    private static final AtomicBoolean wasScheduled = new AtomicBoolean(false);

    private final Consumer<? super VastTransaction> keepAliveAction;
    private final boolean enabled;
    private final int interval;

    private final ConcurrentHashMap<VastTransaction, AtomicInteger> activeTransactions = new ConcurrentHashMap<>(1);
    // TODO - make notify events lock-less
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock wLock = lock.writeLock();
    private final ReentrantReadWriteLock.ReadLock rLock = lock.readLock();

    JobEventService(VastConfig vastConf, Consumer<? super VastTransaction> action)
    {
        this.enabled = vastConf.getVastTransactionKeepAliveEnabled();
        this.interval = vastConf.getVastTransactionKeepAliveIntervalSeconds();
        this.keepAliveAction = action;
    }

    @Override
    public void run()
    {
        LOG.info("Started running");
        long startTime = System.nanoTime();
        try {
            Set<VastTransaction> txForKA = null;
            if (wLock.tryLock(1, TimeUnit.SECONDS)) {
                activeTransactions.entrySet().removeIf(e -> e.getValue().get() <= 0);
                LOG.info("Acquired lock, current keep-alive backlog: %s", activeTransactions);
                txForKA = new HashSet<>(activeTransactions.keySet());
                wLock.unlock();
            }
            else {
                LOG.warn("Failed acquiring lock");
            }
            if (txForKA != null && !txForKA.isEmpty()) {
                txForKA.forEach(keepAliveAction);
            }
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        LOG.info("Exiting. Run took: %s ns", System.nanoTime() - startTime);
    }

    private void start()
    {
        if (this.enabled) {
            if (!wasScheduled.getAndSet(true)) {
                LOG.info("Scheduling self to run in background with interval: %s seconds", interval);
                daemon.scheduleAtFixedRate(this, interval, interval, TimeUnit.SECONDS);
            }
        }
    }

    public void notifyTxActivityStart(VastTransaction newTx)
    {
        LOG.info("notifyTxActivityStart: %s", newTx);
        if (enabled) {
            boolean locked = false;
            start();
            try {
                if (rLock.tryLock(1, TimeUnit.SECONDS)) {
                    locked = true;
                    activeTransactions.compute(newTx, REF_COUNT_INCREASE);
                }
                else {
                    LOG.error("Failed acquiring lock for tx activity end event: %s", newTx);
                }
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            finally {
                if (locked) {
                    rLock.unlock();
                }
            }
        }
    }

    public void notifyTxActivityEnd(VastTransaction tx)
    {
        LOG.info("notifyTxActivityEnd: %s", tx);
        if (enabled) {
            boolean locked = false;
            try {
                if (rLock.tryLock(1, TimeUnit.SECONDS)) {
                    locked = true;
                    if (activeTransactions.computeIfPresent(tx, REF_COUNT_DECREASE) == null) {
                        LOG.warn("Transaction %s was not found", tx);
                    }
                }
                else {
                    LOG.error("Failed acquiring lock for tx activity end event: %s", tx);
                }
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            finally {
                if (locked) {
                    rLock.unlock();
                }
            }
        }
    }

    @VisibleForTesting
    public ConcurrentHashMap<VastTransaction, AtomicInteger> getActiveTransactions()
    {
        boolean locked = false;
        try {
            if (wLock.tryLock(1, TimeUnit.SECONDS)) {
                locked = true;
                return new ConcurrentHashMap<>(activeTransactions);
            }
            else {
                throw new RuntimeException("Failed acquiring write lock for 1 sec");
            }
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (locked) {
                wLock.unlock();
            }
        }
    }
}
