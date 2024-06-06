/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.lang.String.format;

public class FunctionalQ<T>
        implements Supplier<T>, Consumer<T>, WriteExecutionComponent
{
    private static final Logger LOG = LoggerFactory.getLogger(FunctionalQ.class);
    private static final int qOfferTimeoutMs = 10 * 1000;
    private final LinkedBlockingQueue<T> objectsQueue;
    private final int ordinal;
    private final int qPollTimeout;
    private final Predicate<WriteExecutionComponent> noMoreElementsToAccept;
    private final String name;

    // typeClass is passed only for logging type class at runtime
    public FunctionalQ(Class<T> typeClass, String traceToken, int ordinal, int qPollTimeoutMs, int qSize, Predicate<WriteExecutionComponent> noMoreElementsToAccept) {
        name = format("FunctionalQ<%s>%s", typeClass.getSimpleName(), traceToken);
        this.ordinal = ordinal;
        this.qPollTimeout = qPollTimeoutMs;
        objectsQueue = new LinkedBlockingQueue<>(qSize);
        this.noMoreElementsToAccept = noMoreElementsToAccept;
    }

    @Override
    public void accept(T t)
    {
        try {
            while (!this.objectsQueue.offer(t, qOfferTimeoutMs, TimeUnit.MILLISECONDS)) {
                LOG.warn("{} is full for {} ms, retrying offer", name(), qOfferTimeoutMs);
            }
        }
        catch (InterruptedException e) {
            throw new RuntimeException(format("Interrupted while offering object to %s", name()), e);
        }
    }

    @Override
    public T get()
    {
        try {
            T poll;
            do {
                poll = objectsQueue.poll(qPollTimeout, TimeUnit.MILLISECONDS);
                if (poll == null) {
                    LOG.info("{} is empty for {} ms, retrying poll", name(), qPollTimeout);
                }
            }
            while (poll == null && !noMoreElementsToAccept.test(this));
            if (noMoreElementsToAccept.test(this) && poll == null) { // once more to make sure the Q is drained in case of race conditions
                return objectsQueue.poll(qPollTimeout, TimeUnit.MILLISECONDS);
            }
            return poll;
        }
        catch (InterruptedException e) {
            throw new RuntimeException(format("Interrupted while polling for next insert chunk from %s", name()), e);
        }
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public int ordinal()
    {
        return ordinal;
    }
}
