/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx.ka;

import com.vastdata.client.VastConfig;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTransaction;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestJobEventService
{
    private final VastTransaction tx = new SimpleVastTransaction();
    private JobEventService unit;

    @Test
    public void testGetActiveTransactions()
            throws InterruptedException
    {
        VastConfig vastConfig = new VastConfig();
        vastConfig.setVastTransactionKeepAliveIntervalSeconds(1);
        Consumer<? super VastTransaction> testAction = tx -> {};
        unit = new JobEventService(vastConfig, testAction);
        assertTrue(unit.getActiveTransactions().isEmpty());
        ExecutorService threadPool = Executors.newFixedThreadPool(10);
        IntStream.range(0, 100).forEach(i -> threadPool.submit(() -> {
            unit.notifyTxActivityStart(tx);
            assertFalse(unit.getActiveTransactions().isEmpty());
            assertTrue(unit.getActiveTransactions().get(tx).get() > 0);
            int backoff = new Random().nextInt(100) + 50;
            try {
                Thread.sleep(backoff);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            unit.notifyTxActivityEnd(tx);
        }));
        threadPool.shutdown();
        assertTrue(threadPool.awaitTermination(10, TimeUnit.SECONDS));
        ConcurrentHashMap<VastTransaction, AtomicInteger> activeTransactions = unit.getActiveTransactions();
        assertTrue(activeTransactions.isEmpty() || activeTransactions.get(tx).get() == 0, format("%s", activeTransactions));
    }
}
