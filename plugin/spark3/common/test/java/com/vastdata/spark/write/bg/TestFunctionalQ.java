/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestFunctionalQ
{
    @Test
    public void testGetAcceptRace()
    {
        AtomicBoolean once = new AtomicBoolean(false);
        Predicate<WriteExecutionComponent> myPredicate = c -> {
            if (!once.get()) {
                try {
                    synchronized (once) {
                        System.out.println("predicate notifying - first");
                        once.notifyAll();
                        System.out.println("predicate is waiting");
                        once.wait();
                        System.out.println("predicate notifying - second");
                        once.notifyAll();
                    }
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                finally {
                    once.set(true);
                }
            }
            System.out.println("predicate exits");
            return true;
        };
        FunctionalQ<Integer> unit = new FunctionalQ<>(Integer.class, "race", 0, 10, 100, myPredicate);
        new Thread(() -> {
            try {
                synchronized (once) {
                    System.out.println("race thread is waiting");
                    once.wait();
                    System.out.println("race thread was notified");
                    unit.accept(7);
                    unit.accept(8);
                    unit.accept(9);
                    System.out.println("race thread notifying");
                    once.notifyAll();
                    System.out.println("race thread exits");
                }
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        assertEquals((int) unit.get(), 7);
        assertEquals((int) unit.get(), 8);
        assertEquals((int) unit.get(), 9);
        assertNull(unit.get());
    }
}
