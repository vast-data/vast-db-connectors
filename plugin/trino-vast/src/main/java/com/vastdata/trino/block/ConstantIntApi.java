/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

class ConstantIntApi
        implements IntBlockApi
{
    private final int value;

    ConstantIntApi(int value)
    {
        this.value = value;
    }

    @Override
    public int getInt(int position)
    {
        return value;
    }
}
