/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.executor;

import com.amazonaws.http.HttpMethodName;
import com.google.common.collect.ImmutableList;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastClientForTests;
import com.vastdata.client.importdata.EvenSizeWithLimitChunkifier;
import com.vastdata.client.importdata.ImportDataExecutor;
import com.vastdata.client.schema.ImportDataContext;
import com.vastdata.client.schema.ImportDataFile;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.mockserver.VastMockS3Server;
import com.vastdata.mockserver.VastRootHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.vastdata.client.executor.WorkLoad.THREAD_POOL_TERMINATION_TIMEOUT_DEFAULT;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestWorkLoad
{
    @Test(dataProvider = "testLimits")
    public void testExecuteWorkLoadRetries(int numberOfObjects, int numberOfRetries)
    {
        URI uri1 = URI.create("http://localhost:8080");
        URI uri2 = URI.create("http://127.0.0.1:8080");

        AtomicInteger successCtr = new AtomicInteger(0);
        Predicate<Integer> successConsumer = i -> {
            successCtr.incrementAndGet();
            return i % 2 != 0;
        };
        BiConsumer<Throwable, URI> failedConsumer = (o, u) -> {};
        BooleanSupplier bla = () -> true;
        BiFunction<Integer, URI, Integer> bif = (i, uri) -> i;

        List<Integer> collect = IntStream.range(0, numberOfObjects).boxed().collect(Collectors.toList());
        Supplier<Function<URI, Integer>> workSupplier = WorkFactory.fromCollection(collect, bif);
        WorkLoad<Integer> unit = new WorkLoad.Builder<Integer>()
                .setThreadsPrefix("test")
                .setTraceToken(new VastTraceToken(Optional.empty(), 123L, 1))
                .setEndpoints(ImmutableList.of(uri1, uri2))
                .setWorkConsumers(successConsumer, failedConsumer)
                .setWorkSupplier(workSupplier)
                .setRetryStrategy(() -> getRetryStrategy(numberOfRetries))
                .setCircuitBreaker(bla)
                .build();
        unit.executeWorkLoad();
        assertEquals(successCtr.get(), numberOfObjects + (((numberOfObjects / 2) + (numberOfObjects % 2)) * numberOfRetries));
    }

    private RetryStrategy getRetryStrategy(int numberOfRetries)
    {
        return RetryStrategyFactory.fixedSleepBetweenRetries(numberOfRetries, 1);
    }

    @DataProvider
    public Object[][] testLimits()
    {
        return new Object[][] {
                new Object[] {100, 3},
                new Object[] {100, 0},
                new Object[] {0, 5},
        };
    }

    @Test
    public void testEarlyExit()
            throws IOException
    {
        // test WorkLoad exits before request is complete in case of error
        VastRootHandler importSleep = new VastRootHandler();
        String someDest = "buck/schem/tab";
        AtomicInteger ctr = new AtomicInteger(0);
        THREAD_POOL_TERMINATION_TIMEOUT_DEFAULT = 1;
        importSleep.setHook("/" + someDest, HttpMethodName.POST, he -> {
            byte[] dummy = "".getBytes(StandardCharsets.UTF_8);
            if (ctr.getAndIncrement() == 0) {
                try {
                    Thread.sleep(THREAD_POOL_TERMINATION_TIMEOUT_DEFAULT * 2 * 1000); // more than enough
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try {
                    he.sendResponseHeaders(200, 0);
                    try (OutputStream os = he.getResponseBody()) {
                        os.write(dummy);
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            else {
                try {
                    he.sendResponseHeaders(400, 0);
                    try (OutputStream os = he.getResponseBody()) {
                        os.write(dummy);
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            ctr.getAndDecrement(); // expect to decrease only once before test is complete
        });
        try (VastMockS3Server vastMockS3Server = new VastMockS3Server(0, importSleep)) {
            int port = vastMockS3Server.start();
            URI uri1 = URI.create(format("http://localhost:%s", port));
            URI uri2 = URI.create(format("http://127.0.0.1:%s", port));
            VastClient client = VastClientForTests.getVastClient(new JettyHttpClient(), port);
            ImportDataExecutor<VastTransaction> importExecutor = new ImportDataExecutor<>(client);

            VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(ImmutableList.of()), new RootAllocator());
            ImportDataFile importFile = new ImportDataFile("srcBucket", "srcFile", root);
            ImportDataContext ctx = new ImportDataContext(Collections.nCopies(EvenSizeWithLimitChunkifier.CHUNK_SIZE_LIMIT + 1, importFile), someDest); // two chucks
            VastTraceToken token = new VastTraceToken(Optional.empty(), 123456L, 456789);
            VastTransaction mockTrans = new VastTransaction() {
                @Override
                public long getId()
                {
                    return 987L;
                }

                @Override
                public boolean isReadOnly()
                {
                    return false;
                }

                @Override
                public VastTraceToken generateTraceToken(Optional<String> userTraceToken)
                {
                    return token;
                }
            };
            int numberOfRetries = 0;
            try {
                importExecutor.execute(ctx, mockTrans, token, ImmutableList.of(uri1, uri2), () -> getRetryStrategy(numberOfRetries), true);
                fail("Expected an exception");
            }
            catch (Throwable any) {
                assertEquals(ctr.get(), 1);
            }
        }
        finally {
            THREAD_POOL_TERMINATION_TIMEOUT_DEFAULT = 10;
        }
    }
}
