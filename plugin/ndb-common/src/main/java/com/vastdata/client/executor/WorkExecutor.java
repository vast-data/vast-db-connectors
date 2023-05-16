/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.executor;

import java.net.URI;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

class WorkExecutor<T>
        implements Consumer<URI>
{
    private final Function<URI, T> work;
    private final Predicate<T> successConsumer;
    private final Consumer<Throwable> exceptionsConsumer;
    private final RetryStrategy retryStrategy;
    private final Consumer<WorkExecutor<T>> retryConsumer;

    protected WorkExecutor(Function<URI, T> work, Predicate<T> successConsumer, Consumer<Throwable> exceptionsConsumer,
            RetryStrategy retryStrategy, Consumer<WorkExecutor<T>> retryConsumer)
    {
        this.work = work;
        this.successConsumer = successConsumer;
        this.exceptionsConsumer = exceptionsConsumer;
        this.retryStrategy = retryStrategy;
        this.retryConsumer = retryConsumer;
    }

    @Override
    public void accept(URI endpoint)
    {
        try {
            if (!this.successConsumer.test(this.work.apply(endpoint))) {
                retryConsumer.accept(this);
            }
        }
        catch (Throwable any) {
            this.exceptionsConsumer.accept(any);
        }
    }

    public RetryStrategy getRetryStrategy()
    {
        return retryStrategy;
    }
}
