/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.executor;

import com.vastdata.client.error.VastExceptionFactory;

import java.net.URI;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;

public class FailedWorkRetryConsumer<T>
        implements Consumer<WorkExecutor<T>>
{
    private final Consumer<WorkExecutor<T>> retryAction;
    private final BiConsumer<Throwable, URI> exceptionHandler;

    public FailedWorkRetryConsumer(Consumer<WorkExecutor<T>> retryAction, BiConsumer<Throwable, URI> exceptionHandler)
    {
        this.retryAction = retryAction;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public void accept(WorkExecutor<T> workExecutor)
    {
        RetryStrategy retryStrategy = workExecutor.getRetryStrategy();
        if (retryStrategy.shouldRetry()) {
            try {
                Thread.sleep(retryStrategy.getWaitingPeriodMillis());
            }
            catch (InterruptedException e) {
                throw toRuntime(e);
            }
            retryAction.accept(workExecutor);
        }
        else {
            exceptionHandler.accept(VastExceptionFactory.maxRetries(retryStrategy.getCurrentRetryCount()), null);
        }
    }
}
