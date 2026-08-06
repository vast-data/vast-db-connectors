/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastUserException;
import net.bytebuddy.ClassFileVersion;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestVastBGWriter
{
    public static final String TRACE_TOKEN = "TestTraceToken";

    static {
        ClassFileVersion.ofThisVm(); // for bytebuddy dependency
    }

    @Mock VectorSchemaRoot mockChunk;
    @Mock VastWriteStrategy mockWriteStrategy;

    private AutoCloseable autoCloseable;

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        autoCloseable.close();
    }

    @BeforeMethod
    public void setup()
    {
        autoCloseable = MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testDoRunGraceful()
            throws InterruptedException
    {
        int numberOfChunks = 2;
        AtomicInteger chunksCtr = new AtomicInteger(0);
        Supplier<VectorSchemaRoot> chunksSupplier = () -> {
            int b4Inc = chunksCtr.getAndIncrement();
            if (b4Inc < numberOfChunks) {
                return mockChunk;
            }
            else {
                return null;
            }
        };
        VastBGWriter unit = new VastBGWriter(1, TRACE_TOKEN, chunksSupplier,
                mockWriteStrategy);
        AwaitableCompletionListener awaitableCompletionListener = new AwaitableCompletionListener(
                1);
        int expectedNumberOfPolls = numberOfChunks + 2; // one for the null, one for the memory leak prevention
        runUnit(awaitableCompletionListener, unit, expectedNumberOfPolls,
                chunksCtr);
    }

    @Test(expectedExceptions = RuntimeException.class,
            expectedExceptionsMessageRegExp = "Simulation of exception while polling")
    public void testDoRunPollingThrowsException()
            throws InterruptedException
    {
        int numberOfChunks = 2;
        AtomicInteger chunksCtr = new AtomicInteger(0);
        Supplier<VectorSchemaRoot> chunksSupplier = () -> {
            if (chunksCtr.getAndIncrement() < numberOfChunks - 1) {
                return mockChunk;
            }
            else {
                throw new RuntimeException(
                        "Simulation of exception while polling");
            }
        };
        VastBGWriter unit = new VastBGWriter(1, TRACE_TOKEN, chunksSupplier,
                mockWriteStrategy);
        AwaitableCompletionListener awaitableCompletionListener = new AwaitableCompletionListener(
                1);
        int expectedNumberOfPolls = numberOfChunks + 1; // plus one for the memory leak prevention
        runUnit(awaitableCompletionListener, unit, expectedNumberOfPolls,
                chunksCtr);
        awaitableCompletionListener.assertFailure();
    }

    private Supplier<VectorSchemaRoot> singleChunkSupplier()
    {
        AtomicInteger count = new AtomicInteger(0);
        return () -> count.getAndIncrement() == 0 ? mockChunk : null;
    }

    @Test
    public void testChunkOwnershipOnSuccess()
            throws InterruptedException, VastException
    {
        VastBGWriter unit = new VastBGWriter(1, TRACE_TOKEN,
                singleChunkSupplier(), mockWriteStrategy);
        AwaitableCompletionListener listener = new AwaitableCompletionListener(
                1);
        unit.registerCompletionListener(listener);
        new Thread(unit).start();
        listener.await();

        assertTrue(unit.isDone());
        Mockito.verify(mockWriteStrategy, Mockito.times(1)).write(mockChunk);
        Mockito.verify(mockChunk, Mockito.never()).close();
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testChunkOwnershipOnVastException()
            throws InterruptedException, VastException
    {
        Mockito
                .doThrow(new VastUserException(
                        "simulated VastException from write"))
                .when(mockWriteStrategy)
                .write(any(VectorSchemaRoot.class));

        VastBGWriter unit = new VastBGWriter(1, TRACE_TOKEN,
                singleChunkSupplier(), mockWriteStrategy);
        AwaitableCompletionListener listener = new AwaitableCompletionListener(
                1);
        unit.registerCompletionListener(listener);
        new Thread(unit).start();
        listener.await();

        assertTrue(unit.isDone());
        Mockito.verify(mockChunk, Mockito.times(1)).close();
        listener.assertFailure();
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "simulated RuntimeException from write")
    public void testChunkOwnershipOnRuntimeException()
            throws InterruptedException, VastException
    {
        Mockito
                .doThrow(new RuntimeException(
                        "simulated RuntimeException from write"))
                .when(mockWriteStrategy)
                .write(any(VectorSchemaRoot.class));

        VastBGWriter unit = new VastBGWriter(1, TRACE_TOKEN,
                singleChunkSupplier(), mockWriteStrategy);
        AwaitableCompletionListener listener = new AwaitableCompletionListener(
                1);
        unit.registerCompletionListener(listener);
        new Thread(unit).start();
        listener.await();

        assertTrue(unit.isDone());
        Mockito.verify(mockChunk, Mockito.times(1)).close();
        listener.assertFailure();
    }

    private void runUnit(AwaitableCompletionListener listener,
            VastBGWriter unit, int expectedNumberOfPolls,
            AtomicInteger chunksCtr)
            throws InterruptedException
    {
        unit.registerCompletionListener(listener);
        new Thread(unit).start();
        listener.await();
        assertTrue(unit.isDone());
        assertEquals(chunksCtr.get(), expectedNumberOfPolls);
    }
}
