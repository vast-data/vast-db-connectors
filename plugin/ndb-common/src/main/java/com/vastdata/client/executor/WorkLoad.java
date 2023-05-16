/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.executor;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.vastdata.client.tx.VastTraceToken;
import io.airlift.log.Logger;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class WorkLoad<T>
{
    private static final Logger LOG = Logger.get(WorkLoad.class);
    protected static long THREAD_POOL_TERMINATION_TIMEOUT_DEFAULT = 10;
    private final List<URI> endpoints;
    private final ExecutorService executorService;
    private final Consumer<URI> endPointWorkingThreadSubmitter;
    private final Supplier<Future<?>> workSubmitterThreadStarter;
    private final VastTraceToken traceToken;
    private final BooleanSupplier circuitBreaker;
    private final long threadPoolTerminationTimeout = THREAD_POOL_TERMINATION_TIMEOUT_DEFAULT;

    private WorkLoad(Supplier<Function<URI, T>> workSupplier, BooleanSupplier circuitBreaker,
            Predicate<T> successfulWorkConsumer, Consumer<Throwable> workExecutionExceptionsHandler,
            Supplier<RetryStrategy> retryStrategySupplier, List<URI> endpoints, String workLoadThreadsPrefix, VastTraceToken traceToken)
    {
        this.traceToken = traceToken;
        this.circuitBreaker = circuitBreaker;
        LinkedBlockingDeque<WorkExecutor<T>> workQueue = new LinkedBlockingDeque<>();
        this.endpoints = endpoints;
        String nameFormat = getThreadNameFormat(workLoadThreadsPrefix, traceToken);
        this.executorService = Executors.newFixedThreadPool(endpoints.size() + 1, new ThreadFactoryBuilder().setNameFormat(nameFormat).build());
        // failed work objects - sleep and enqueue as first
        Consumer<WorkExecutor<T>> workRetryConsumer = new FailedWorkRetryConsumer<>(workQueue::addFirst, workExecutionExceptionsHandler);
        // single thread to poll from work supplier and add WorkExecutor objects to queue
        workSubmitterThreadStarter = () -> executorService.submit(new WorkSubmitter<>(workSupplier, successfulWorkConsumer,
                workExecutionExceptionsHandler, workRetryConsumer, retryStrategySupplier, circuitBreaker, workQueue));
        // thread per endpoint - poll WorkExecutor objects and execute them on thread's endpoint
        endPointWorkingThreadSubmitter = endpoint -> executorService.submit(new SubmittedWorkProcessor<>(endpoint, workQueue, circuitBreaker));
    }

    private String getThreadNameFormat(String workLoadThreadsPrefix, VastTraceToken traceToken)
    {
        String prefixWithTraceToken = format(Strings.isNullOrEmpty(workLoadThreadsPrefix) ? "work-load-thread-%s" : workLoadThreadsPrefix + "-thread-%s", traceToken);
        return prefixWithTraceToken + "-%s";
    }

    public void executeWorkLoad()
    {
        LOG.info("(%s) Starting work load processing", traceToken);
        Future<?> workSubmitter = workSubmitterThreadStarter.get();
        endpoints.forEach(endPointWorkingThreadSubmitter);
        try {
            workSubmitter.get();
        }
        catch (InterruptedException | ExecutionException e) {
            executorService.shutdownNow();
            throw toRuntime(e);
        }
        LOG.debug("(%s) Done adding to work queue", traceToken);
        executorService.shutdown();
        boolean done = false;
        while (!done) {
            LOG.debug("(%s) Waiting for all work to complete", traceToken);
            if (!circuitBreaker.getAsBoolean()) {
                LOG.warn("(%s) Received signal to end work load execution prematurely", traceToken);
                LOG.warn("(%s) Attempted to stop all running tasks. %s tasks have not been started", traceToken, executorService.shutdownNow().size());
            }
            try {
                done = executorService.awaitTermination(threadPoolTerminationTimeout, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                executorService.shutdownNow();
                throw toRuntime(e);
            }
        }
        LOG.debug("(%s) Done processing all work", traceToken);
    }

    public static class Builder<T>
    {
        private Supplier<Function<URI, T>> workSupplier;
        private Supplier<RetryStrategy> retryStrategy;
        private BooleanSupplier circuitBreaker;
        private Predicate<T> successConsumer;
        private Consumer<Throwable> exceptionsHandler;
        private List<URI> endpoints;
        private String threadsPrefix;
        private VastTraceToken traceToken;

        public WorkLoad<T> build()
        {
            requireNonNull(workSupplier, "Work supplier is null");
            requireNonNull(successConsumer, "Successful work consumer is null");
            requireNonNull(exceptionsHandler, "Failed work consumer is null");
            requireNonNull(endpoints, "Endpoint list is null");
            requireNonNull(circuitBreaker, "Circuit breaker is null");
            requireNonNull(traceToken, "Trace token is null");
            return new WorkLoad<>(workSupplier, circuitBreaker, successConsumer, exceptionsHandler, retryStrategy, endpoints, threadsPrefix, traceToken);
        }

        public Builder<T> setEndpoints(List<URI> endpoints)
        {
            this.endpoints = endpoints;
            return this;
        }

        public Builder<T> setThreadsPrefix(String threadsPrefix)
        {
            this.threadsPrefix = threadsPrefix;
            return this;
        }

        public Builder<T> setCircuitBreaker(BooleanSupplier circuitBreaker)
        {
            this.circuitBreaker = circuitBreaker;
            return this;
        }

        public Builder<T> setWorkSupplier(Supplier<Function<URI, T>> workSupplier)
        {
            this.workSupplier = workSupplier;
            return this;
        }

        public Builder<T> setRetryStrategy(Supplier<RetryStrategy> retryStrategy)
        {
            this.retryStrategy = retryStrategy;
            return this;
        }

        public Builder<T> setWorkConsumers(Predicate<T> successConsumer, Consumer<Throwable> exceptionsHandler)
        {
            this.successConsumer = successConsumer;
            this.exceptionsHandler = exceptionsHandler;
            return this;
        }

        public Builder<T> setTraceToken(VastTraceToken traceToken)
        {
            this.traceToken = traceToken;
            return this;
        }
    }
}
