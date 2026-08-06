/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.vastdata.client.VastConfig;
import io.airlift.log.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class ShapingLoggerFactory
{
    private final Map<Class<?>, ShapingLogger> instances;
    private final VastConfig vastConfig;

    @Inject
    public ShapingLoggerFactory(VastConfig vastConfig)
    {
        this.vastConfig = vastConfig;
        instances = new ConcurrentHashMap<>();
    }

    public ShapingLogger getInstance(Class<?> clazz, Logger logger)
    {
        return instances.computeIfAbsent(clazz, v -> new ShapingLogger(logger,
                vastConfig.getShapingLoggerThreshold(),
                vastConfig.getShapingLoggerDuration(),
                vastConfig.getShapingLoggerNumberOfSamples()));
    }
}
