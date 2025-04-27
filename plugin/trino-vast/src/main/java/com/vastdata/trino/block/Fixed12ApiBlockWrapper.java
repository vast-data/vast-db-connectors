/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

import java.util.function.Function;

class Fixed12ApiBlockWrapper implements Fixed12BlockApi
{
    private final Function<Integer, Long> dictIdFirstFunction;
    private final Function<Integer, Integer> dictIdSecondFunction;

    public Fixed12ApiBlockWrapper(Function<Integer, Long> dictIdFirstFunction, Function<Integer, Integer> dictIdSecondFunction)
    {
        this.dictIdFirstFunction = dictIdFirstFunction;
        this.dictIdSecondFunction = dictIdSecondFunction;
    }

    @Override
    public long getFixed12First(int position)
    {
        return dictIdFirstFunction.apply(position);
    }

    @Override
    public int getFixed12Second(int position)
    {
        return dictIdSecondFunction.apply(position);
    }
}
