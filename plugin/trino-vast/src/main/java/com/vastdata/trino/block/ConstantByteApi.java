/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

class ConstantByteApi
        implements ByteBlockApi
{
    private final byte value;

    ConstantByteApi(byte value)
    {
        this.value = value;
    }

    @Override
    public byte getByte(int position)
    {
        return value;
    }
}
