/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

import java.util.function.Function;

class LongApiBlockWrapper
        implements LongBlockApi
{
    private final Function<Integer, Long> longFunction;

    public LongApiBlockWrapper(Function<Integer, Long> longFunction)
    {
        this.longFunction = longFunction;
    }

    @Override
    public Long getLong(int position)
    {
        return longFunction.apply(position);
    }
}
