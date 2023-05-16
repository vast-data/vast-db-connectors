/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.executor;

import io.airlift.log.Logger;

import java.net.URI;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;

public class SubmittedWorkProcessor<T>
        implements Runnable
{
    private static final Logger LOG = Logger.get(SubmittedWorkProcessor.class);
    private final URI endpoint;
    private final LinkedBlockingDeque<WorkExecutor<T>> workQueue;
    private final BooleanSupplier circuitBreaker;

    public SubmittedWorkProcessor(URI endpoint, LinkedBlockingDeque<WorkExecutor<T>> workQueue, BooleanSupplier circuitBreaker)
    {
        this.endpoint = endpoint;
        this.workQueue = workQueue;
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    public void run()
    {
        WorkExecutor<T> element;
        int processed = 0;
        try {
            LOG.debug("Work processing thread for endpoint %s is starting", endpoint);
            while ((element = workQueue.pollFirst(500, TimeUnit.MILLISECONDS)) != null) {
                if (!circuitBreaker.getAsBoolean()) {
                    LOG.debug("Work processing thread for endpoint %s received signal to finish processing jobs prematurely", endpoint);
                    break;
                }
                element.accept(endpoint);
                processed++;
            }
            LOG.debug("Work processing thread for endpoint %s is finished. Processed %s work objects", endpoint, processed);
        }
        catch (InterruptedException ie) {
            LOG.error(ie, "Work processing thread for endpoint %s interrupted while polling for next work", endpoint);
            throw toRuntime(ie);
        }
    }
}
