/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastException;
import com.vastdata.client.tx.SimpleVastTransaction;
import net.bytebuddy.ClassFileVersion;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestVastBGWriter
{
    static {
        ClassFileVersion.ofThisVm(); // for bytebuddy dependency
    }

    private static final URI uri;

    static {
        try {
            uri = new URI("http://localhost:8080");
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static final SimpleVastTransaction TX = new SimpleVastTransaction(5, false, false);

    public static final String TABLE_NAME = "table";
    public static final String SCHEMA_NAME = "schema";
    public static final String TRACE_TOKEN = "TestTraceToken";
    @Mock VectorSchemaRoot mockChunk;
    @Mock VastClient mockClient;
    @Mock VastConfig mockCfg;
    private Function<VastConfig, VastClient> mockClientSupplier;

    private AutoCloseable autoCloseable;

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        autoCloseable.close();
    }

    @BeforeMethod
    public void setup()
            throws VastException
    {
        autoCloseable = MockitoAnnotations.openMocks(this);
        mockClientSupplier = cfg -> mockClient;
        Mockito.doNothing().when(mockClient).insertRows(ArgumentMatchers.any(SimpleVastTransaction.class), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.any(VectorSchemaRoot.class), ArgumentMatchers.any(URI.class), ArgumentMatchers.any(Optional.class));
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
        VastBGWriter unit = VastBGWriterFactory.forInsert(1, mockClientSupplier, TRACE_TOKEN, mockCfg, uri, TX, SCHEMA_NAME, TABLE_NAME, chunksSupplier, false);
        AwaitableCompletionListener awaitableCompletionListener = new AwaitableCompletionListener(1);
        int expectedNumberOfPolls = numberOfChunks + 2; // one for the null, one for the memory leak prevention
        runUnit(awaitableCompletionListener, unit, expectedNumberOfPolls, chunksCtr);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Simulation of exception while polling")
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
                throw new RuntimeException("Simulation of exception while polling");
            }
        };
        VastBGWriter unit = VastBGWriterFactory.forInsert(1, mockClientSupplier, TRACE_TOKEN, mockCfg, uri, TX, SCHEMA_NAME, TABLE_NAME, chunksSupplier, false);
        AwaitableCompletionListener awaitableCompletionListener = new AwaitableCompletionListener(1);
        int expectedNumberOfPolls = numberOfChunks + 1; // plus one for the memory leak prevention
        runUnit(awaitableCompletionListener, unit, expectedNumberOfPolls, chunksCtr);
        awaitableCompletionListener.assertFailure();
    }

    private void runUnit(AwaitableCompletionListener listener, VastBGWriter unit, int expectedNumberOfPolls, AtomicInteger chunksCtr)
            throws InterruptedException
    {
        unit.registerCompletionListener(listener);
        new Thread(unit).start();
        listener.await();
        assertTrue(unit.isDone());
        assertEquals(chunksCtr.get(), expectedNumberOfPolls);
    }
}
