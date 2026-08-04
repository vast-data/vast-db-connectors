/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata;

import io.airlift.log.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * this Logger counts similar logs and only outputs a single log message based
 * on threshold / intervalInMillis
 */
public class ShapingLogger
{
    private static final String CATALOG_FORMAT = "catalog[%s]: ";
    private static final String QUERY_FORMAT = CATALOG_FORMAT + "queryId[%s]: ";
    private static final String FORMAT = "%s - skipped %d times";
    private final Map<String, ShapingLoggerState> shapingLoggerStateMap = new ConcurrentHashMap<>();

    private final Logger logger;

    //threshold for log flushing. if threshold<=0 then threshold is ignored
    private final int threshold;
    //if true first log message will be logged regardless of interval/duration
    private final int numberOfSamples;
    //interval in millis for log flushing. if duration = Duration.ZERO then it is ignored
    long durationMillis;

    public ShapingLogger(Logger logger, int threshold, Duration duration,
            int numberOfSamples)
    {
        this.logger = requireNonNull(logger);
        this.threshold = threshold;
        durationMillis = (duration != null ?
                duration :
                Duration.ZERO).toMillis();
        this.numberOfSamples = numberOfSamples;
        checkArgument(threshold == 0 || threshold > numberOfSamples,
                "threshold must be greater than number of samples");
    }

    public void info(String message)
    {
        info("%s", message);
    }

    public void info(final String format, Object... args)
    {
        if (logger.isInfoEnabled()) {
            log(format, () -> logger.info(String.format(format, args)));
        }
    }

    public void debug(final String format, Object... args)
    {
        if (logger.isDebugEnabled()) {
            log(format, () -> logger.debug(String.format(format, args)));
        }
    }

    public void warn(String message)
    {
        warn("%s", message);
    }

    public void warn(final String format, Object... args)
    {
        log(format, () -> logger.warn(String.format(format, args)));
    }

    public void warn(Throwable exception, final String format, Object... args)
    {
        log(format, () -> logger.warn(exception, String.format(format, args)));
    }

    public void error(String message)
    {
        error("%s", message);
    }

    public void error(final String format, Object... args)
    {
        log(format, () -> logger.error(String.format(format, args)));
    }

    public void error(Throwable e, final String format, Object... args)
    {
        log(format, () -> logger.error(e, String.format(format, args)));
    }

    private void log(String key, Runnable runnable)
    {
        long currentTimeMillis = System.currentTimeMillis();
        shapingLoggerStateMap.compute(key, (pair, val) -> {
            if (val == null) {
                val = new ShapingLoggerState(1, currentTimeMillis);
            }

            long lastLogTime = val.lastLogTime();
            int count = val.count();
            if (count <= numberOfSamples || threshold == 0) {
                runnable.run();
            }

            if ((threshold > 0 && (count == threshold)) || ((durationMillis > 0 && threshold > 0) && currentTimeMillis - lastLogTime > durationMillis)) {
                if (count > numberOfSamples) {
                    logger.info(getOccurrencesMessage(key,
                            count - numberOfSamples));
                }
                lastLogTime = currentTimeMillis;
                count = 0;
            }
            return new ShapingLoggerState(count + 1, lastLogTime);
        });
    }

    private Pair<String, List<Object>> getKey(String message, Object... args)
    {
        return Pair.of(message, List.of());
    }

    private String getOccurrencesMessage(String key, int count)
    {
        return String.format(FORMAT, key, count);
    }

    private static class ShapingLoggerState
    {
        private final int count;
        private final long lastLogTime;

        private ShapingLoggerState(int count, long lastLogTime)
        {
            this.count = count;
            this.lastLogTime = lastLogTime;
        }

        public int count()
        {
            return count;
        }

        public long lastLogTime()
        {
            return lastLogTime;
        }
    }
}
