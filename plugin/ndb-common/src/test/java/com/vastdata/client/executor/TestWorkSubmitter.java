/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.executor;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.testng.Assert.assertEquals;

public class TestWorkSubmitter
{
    private final int numOfTestObjects = 5;
    private AtomicInteger numberOfSuppliedObjects;

    private final Supplier<Function<URI, Object>> supplier = () -> {
        if (numberOfSuppliedObjects.get() < numOfTestObjects) {
            numberOfSuppliedObjects.incrementAndGet();
            return uri -> new Object();
        }
        else {
            return null;
        }
    };

    @BeforeMethod
    public void setup()
    {
        numberOfSuppliedObjects = new AtomicInteger(0);
    }

    @Test
    public void testCircuitBreaker()
            throws InterruptedException
    {
        int expectedQueueSize = 2;
        int expectedSuppliedObjects = 3;
        BooleanSupplier breaker = () -> numberOfSuppliedObjects.get() <= expectedQueueSize;
        runTest(expectedQueueSize, expectedSuppliedObjects, breaker);
    }

    @Test
    public void testGracefulSubmit()
            throws InterruptedException
    {
        BooleanSupplier breaker = () -> true;
        runTest(numOfTestObjects, numOfTestObjects, breaker);
    }

    private void runTest(int expectedQueueSize, int expectedNumberOfSubmittedObjects, BooleanSupplier breaker)
            throws InterruptedException
    {
        LinkedBlockingDeque<WorkExecutor<Object>> queue = new LinkedBlockingDeque<>(1);
        AtomicInteger polled = new AtomicInteger(0);
        Thread queuePoller = new Thread(() -> {
            try {
                while (queue.poll(1, TimeUnit.SECONDS) != null) {
                    polled.incrementAndGet();
                }
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        queuePoller.start();
        WorkSubmitter<Object> unit = new WorkSubmitter<>(supplier, null, null, null, () -> null, breaker, queue);
        unit.run();
        queuePoller.join(100);
        assertEquals(numberOfSuppliedObjects.get(), expectedNumberOfSubmittedObjects);
        assertEquals(polled.get(), expectedQueueSize);
    }
}
