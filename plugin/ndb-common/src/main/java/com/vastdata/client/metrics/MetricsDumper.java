/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.vastdata.client.VastConfig;
import io.airlift.log.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsDumper
{
    private static final Logger dumpLogger = Logger.get("METRICS-DUMP");

    private final long dumpIntervalSeconds;

    private final AtomicLong lastDumpTime = new AtomicLong(0);
    private final AtomicInteger activeQueries = new AtomicInteger(0);

    private final Map<Class<VastMetrics<?>>, VastMetrics<?>> metricsMap;
    private final Map<Class<VastMetrics<?>>, VastMetrics<?>> lastMetricsMap = new HashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();

    @Inject
    public MetricsDumper(Set<VastMetrics> metrics, VastConfig config)
    {
        this.dumpIntervalSeconds = config.getMetricDumperInterval().getSeconds();
        dumpLogger
                .info("Starting MetricsDumper for %d metrics", metrics.size());
        this.metricsMap = metrics
                .stream()
                .collect(HashMap::new, (map, metric) -> map
                        .put((Class<VastMetrics<?>>) metric.getClass(), metric),
                        HashMap::putAll);

        ScheduledExecutorService executorService = Executors
                .newScheduledThreadPool(1);
        executorService
                .scheduleAtFixedRate(this::tryDump, 0,
                        config.getMemoryLimiterHangingValidationInterval().getSeconds(), TimeUnit.SECONDS);
    }

    public void tryDump()
    {
        long now = System.currentTimeMillis();
        if (shouldDump(now)) {
            lastDumpTime.set(now);
            dumpMetrics();
        }
    }

    private boolean shouldDump(long now)
    {
        boolean dump = false;

        if (activeQueries.get() > 0) {
            long last = lastDumpTime.get();
            if (now - last >= TimeUnit.SECONDS.toMillis(dumpIntervalSeconds)) {
                dump = true;
            }
        }

        return dump;
    }

    private void dumpMetrics()
    {
        Map<String, String> metricsToDump = new HashMap<>();

        for (Map.Entry<Class<VastMetrics<?>>, VastMetrics<?>> entry : metricsMap
                .entrySet()) {
            Class<VastMetrics<?>> clazz = entry.getKey();
            VastMetrics<?> currentMetrics = entry.getValue();
            VastMetrics<?> lastMetrics = lastMetricsMap.get(clazz);

            getDiffMetrics(metricsToDump, lastMetrics, currentMetrics, clazz);
            getStateMetrics(metricsToDump, currentMetrics, clazz);

            lastMetricsMap.put(clazz, currentMetrics.copy());
        }

        if (!metricsToDump.isEmpty()) {
            try {
                dumpLogger.info("%s", mapper.writeValueAsString(metricsToDump));
            }
            catch (Exception e) {
                dumpLogger.error(e, "Failed to serialize metrics diff");
            }
        }
    }

    private void getDiffMetrics(Map<String, String> metricsToDump,
                                VastMetrics<?> lastMetrics,
                                VastMetrics<?> currentMetrics,
                                Class<?> clazz)
    {
        Map<String, Long> currentMap = currentMetrics.diffMetrics();
        Map<String, Long> lastMap = lastMetrics != null ?
                lastMetrics.diffMetrics() :
                null;

        for (Map.Entry<String, Long> metricEntry : currentMap.entrySet()) {
            String key = metricEntry.getKey();
            Long currentValue = metricEntry.getValue();
            Long lastValue = lastMap != null ?
                    lastMap.getOrDefault(key, 0L) :
                    0L;

            if (currentValue - lastValue != 0) {
                long diff = currentValue - lastValue;
                String prefix = diff > 0 ? "+" : "";
                metricsToDump
                        .put(clazz.getSimpleName() + "::" + key,
                                prefix + diff + " (" + currentValue + ")");
            }
        }
    }

    private void getStateMetrics(Map<String, String> metricsToDump,
            VastMetrics<?> currentMetrics,
            Class<?> clazz)
    {
        Map<String, ?> currentMap = currentMetrics.stateMetrics();

        for (Map.Entry<String, ?> metricEntry : currentMap.entrySet()) {
            String key = metricEntry.getKey();
            Object currentValue = metricEntry.getValue();

            // Only print if the state metric is non-zero
            if (!String.valueOf(currentValue).equals("0")) {
                metricsToDump.put(clazz.getSimpleName() + "::" + key, String.valueOf(currentValue));
            }
        }
    }
}
