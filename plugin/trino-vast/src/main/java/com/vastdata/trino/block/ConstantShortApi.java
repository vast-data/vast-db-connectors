/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

class ConstantShortApi
        implements ShortBlockApi
{
    private final short value;

    ConstantShortApi(short value)
    {
        this.value = value;
    }

    @Override
    public short getShort(int position)
    {
        return value;
    }
}
