/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.metrics;

import java.util.Collections;
import java.util.Map;

public interface VastMetrics<T>
{
    Map<String, Long> asMap();

    Map<String, Long> diffMetrics();

    default Map<String, Long> stateMetrics()
    {
        return Collections.emptyMap();
    }

    default Map<String, Long> durationMetrics()
    {
        return Collections.emptyMap();
    }

    void merge(T other);

    VastMetrics<T> copy();
}
