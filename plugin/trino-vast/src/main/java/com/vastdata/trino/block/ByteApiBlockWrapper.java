/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

import java.util.function.Function;

class ByteApiBlockWrapper
        implements ByteBlockApi
{
    private final Function<Integer, Byte> byteFunction;

    ByteApiBlockWrapper(Function<Integer, Byte> byteFunction)
    {
        this.byteFunction = byteFunction;
    }

    @Override
    public byte getByte(int position)
    {
        return byteFunction.apply(position);
    }
}
