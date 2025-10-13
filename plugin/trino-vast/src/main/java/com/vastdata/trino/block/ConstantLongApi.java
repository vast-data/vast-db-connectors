/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

class ConstantLongApi
        implements LongBlockApi
{
    private final long value;

    ConstantLongApi(long value)
    {
        this.value = value;
    }

    @Override
    public Long getLong(int position)
    {
        return value;
    }
}
