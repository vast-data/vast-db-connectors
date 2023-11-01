/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

public abstract class ParallelWriteExecutionComponent
        implements Runnable, CompletedWriteExecutionComponent
{
    private static final Logger LOG = LoggerFactory.getLogger(ParallelWriteExecutionComponent.class);
    protected final Set<CompletionListener> registeredBlockingCompletionListeners = new HashSet<>(1);
    protected final AtomicBoolean isDone = new AtomicBoolean(false);
    protected final int ordinal;
    protected Throwable runtimeError = null;

    ParallelWriteExecutionComponent(int ordinal) {
        this.ordinal = ordinal;
    }

    public abstract void doRun();
    public abstract String getTaskName();

    @Override
    public String name()
    {
        return getTaskName();
    }

    @Override
    public int ordinal()
    {
        return this.ordinal;
    }

    @Override
    public Status status()
    {
        return new Status(runtimeError == null, runtimeError);
    }

    @Override
    public void run()
    {
        try {
            doRun();
        }
        catch (Throwable e) {
            LOG.error(format("Exception during run of %s", getTaskName()), e);
            this.runtimeError = e;
        }
        finally {
            this.isDone.set(true);
            reportComplete();
        }
    }

    public boolean isDone() {
        return isDone.get();
    }

    public synchronized void registerCompletionListener(CompletionListener listener)
    {
        this.registeredBlockingCompletionListeners.add(listener);
    }


    private synchronized void reportComplete()
    {
        registeredBlockingCompletionListeners.forEach(completionListener -> completionListener.completed(this));
    }
}
