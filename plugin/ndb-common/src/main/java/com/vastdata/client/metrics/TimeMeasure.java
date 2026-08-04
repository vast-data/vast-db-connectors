/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.metrics;

import java.util.function.LongConsumer;

public class TimeMeasure
{
    private long lastStartTime;
    private long lastEndTime;

    public TimeMeasure()
    {
        long now = System.nanoTime();
        this.lastStartTime = now;
        this.lastEndTime = now;
    }

    public static TimeMeasure createAndStart()
    {
        TimeMeasure timeMeasure = new TimeMeasure();
        timeMeasure.start();
        return timeMeasure;
    }

    public void start(LongConsumer idleTimeConsumer)
    {
        long now = System.nanoTime();
        idleTimeConsumer.accept(now - this.lastEndTime);
        this.lastStartTime = now;
    }

    public void start()
    {
        start(l -> {});
    }

    public void end(LongConsumer executionTimeConsumer)
    {
        long now = System.nanoTime();
        executionTimeConsumer.accept(now - this.lastStartTime);
        this.lastEndTime = now;
    }

    public void end()
    {
        end(l -> {});
    }
}
