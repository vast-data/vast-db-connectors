/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.executor;

import io.airlift.log.Logger;

import java.net.URI;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

class WorkSubmitter<T>
        implements Runnable
{
    private static final Logger LOG = Logger.get(WorkSubmitter.class);
    private final Supplier<Function<URI, T>> workSupplier;
    private final Predicate<T> successConsumer;
    private final Consumer<Throwable> exceptionsHandler;
    private final Consumer<WorkExecutor<T>> retryConsumer;
    private final Supplier<RetryStrategy> retryStrategySupplier;
    private final BooleanSupplier circuitBreaker;
    private final LinkedBlockingDeque<WorkExecutor<T>> workQueue;

    WorkSubmitter(Supplier<Function<URI, T>> workSupplier, Predicate<T> successConsumer, Consumer<Throwable> exceptionsHandler,
            Consumer<WorkExecutor<T>> retryConsumer, Supplier<RetryStrategy> retryStrategySupplier,
            BooleanSupplier circuitBreaker, LinkedBlockingDeque<WorkExecutor<T>> workQueue)
    {
        this.workSupplier = workSupplier;
        this.successConsumer = successConsumer;
        this.exceptionsHandler = exceptionsHandler;
        this.retryConsumer = retryConsumer;
        this.retryStrategySupplier = retryStrategySupplier;
        this.circuitBreaker = circuitBreaker;
        this.workQueue = workQueue;
    }

    @Override
    public void run()
    {
        LOG.debug("Starting submitting work");
        int submitted = 0;
        Function<URI, T> nextWork;
        try {
            while ((nextWork = workSupplier.get()) != null) {
                if (!circuitBreaker.getAsBoolean()) {
                    LOG.info("Received signal to finish submitting work prematurely");
                    return;
                }
                WorkExecutor<T> workExecutor = new WorkExecutor<>(nextWork, successConsumer, exceptionsHandler, retryStrategySupplier.get(), retryConsumer);
                try {
                    while (!workQueue.offer(workExecutor, 5, TimeUnit.SECONDS)) {
                        LOG.debug("Work queue is full, waiting for space");
                        if (!circuitBreaker.getAsBoolean()) {
                            LOG.info("Received signal to finish submitting work prematurely");
                            return;
                        }
                    }
                }
                catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted while waiting to submit next work", e);
                }
                submitted++;
            }
        }
        finally {
            LOG.debug("Work submitting thread is exiting. Submitted %s work objects", submitted);
        }
    }
}
