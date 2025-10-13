/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

import java.util.function.Function;

class IntApiBlockWrapper
        implements IntBlockApi
{
    private final Function<Integer, Integer> intFunction;

    IntApiBlockWrapper(Function<Integer, Integer> intFunction)
    {
        this.intFunction = intFunction;
    }

    @Override
    public int getInt(int position)
    {
        return intFunction.apply(position);
    }
}
