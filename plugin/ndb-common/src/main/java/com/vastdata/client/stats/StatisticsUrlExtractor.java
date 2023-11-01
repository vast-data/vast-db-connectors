/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.stats;

import java.util.function.Function;

public abstract class StatisticsUrlExtractor<T>
{
    private final Function<T, String> bucketExtractor;
    private final Function<T, String> handleIdExtractor;

    public StatisticsUrlExtractor(Function<T, String> bucketExtractor, Function<T, String> handleIdExtractor)
    {
        this.bucketExtractor = bucketExtractor;
        this.handleIdExtractor = handleIdExtractor;
    }

    String getBucket(T target)
    {
        return bucketExtractor.apply(target);
    }

    String getHandleID(T target)
    {
        return handleIdExtractor.apply(target);
    }
}
