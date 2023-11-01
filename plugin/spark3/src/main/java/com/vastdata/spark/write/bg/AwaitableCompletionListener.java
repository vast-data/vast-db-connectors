/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static java.lang.String.format;

public class AwaitableCompletionListener
        implements CompletionListener, Predicate<WriteExecutionComponent>
{
    private static final Logger LOG = LoggerFactory.getLogger(AwaitableCompletionListener.class);
    private Optional<Throwable> exception = Optional.empty();
    private final CountDownLatch latch;
    private final AtomicInteger minimalCompletedPhase = new AtomicInteger(-1);
    private final Set<Callable<Void>> failureActions = new HashSet<>();

    private class ThrowableConsumer implements Consumer<Throwable>
    {
        private final AtomicReference<Throwable> suppressed = new AtomicReference<>();
        @Override
        public void accept(Throwable e)
        {
            RuntimeException re = e instanceof RuntimeException ? (RuntimeException) e : toRuntime(e);
            failureActions.forEach(action -> {
                try {
                    action.call();
                }
                catch (Exception toSuppress) {
                    suppressed.set(toSuppress);
                }
            });
            if (suppressed.get() != null) {
                re.addSuppressed(suppressed.get());
            }
            throw re;
        }
    }
    private final Consumer<Throwable> throwableConsumer = new ThrowableConsumer();

    public AwaitableCompletionListener(int expectedNumberOfThreads) {
        this.latch = new CountDownLatch(expectedNumberOfThreads);
    }

    public void await()
            throws InterruptedException
    {
        this.latch.await();
    }

    public synchronized void assertFailure()
    {
        this.exception.ifPresent(throwableConsumer);
    }

    @Override
    public synchronized void completed(CompletedWriteExecutionComponent component)
    {
        logCompleted(component);
        if (component.status().getFailureCause() != null) {
            this.exception = Optional.of(component.status().getFailureCause());
        }
        if (component.ordinal() > this.minimalCompletedPhase.get()) {
            this.minimalCompletedPhase.set(component.ordinal());
        }
        this.latch.countDown();
    }

    private static void logCompleted(CompletedWriteExecutionComponent component)
    {
        LOG.debug("Received completed {} notification from {}",
                component.status().isSuccess() ? "successfully" : "with error",
                format("%s(%s)", component.name(), component.ordinal()));
    }

    @Override
    public synchronized boolean test(WriteExecutionComponent writeExecutionComponent)
    {
        return writeExecutionComponent.ordinal() <= this.minimalCompletedPhase.get();
    }

    public void registerFailureAction(Callable<Void> action)
    {
        failureActions.add(action);
    }
}
