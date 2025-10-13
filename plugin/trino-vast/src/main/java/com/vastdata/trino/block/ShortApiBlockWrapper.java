/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

import java.util.function.Function;

class ShortApiBlockWrapper implements ShortBlockApi
{
    private final Function<Integer, Short> shortFunction;

    ShortApiBlockWrapper(Function<Integer, Short> shortFunction)
    {
        this.shortFunction = shortFunction;
    }

    @Override
    public short getShort(int position)
    {
        return shortFunction.apply(position);
    }
}
