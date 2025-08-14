/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

import java.util.function.Function;

class Int128ApiBlockWrapper implements Int128ArrayBlockApi
{
    private final Function<Integer, Long> dictIdFirstFunction;
    private final Function<Integer, Long> dictIdSecondFunction;

    public Int128ApiBlockWrapper(Function<Integer, Long> dictIdFirstFunction, Function<Integer, Long> dictIdSecondFunction)
    {
        this.dictIdFirstFunction = dictIdFirstFunction;
        this.dictIdSecondFunction = dictIdSecondFunction;
    }

    @Override
    public long getInt128High(int position)
    {
        return dictIdFirstFunction.apply(position);
    }

    @Override
    public long getInt128Low(int position)
    {
        return dictIdSecondFunction.apply(position);
    }
}
