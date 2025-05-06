/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

class ConstantInt128ArrayApi implements Int128ArrayBlockApi
{
    private final long first;
    private final long second;

    ConstantInt128ArrayApi(long first, long second) {
        this.second = second;
        this.first = first;
    }

    @Override
    public long getInt128High(int position)
    {
        return first;
    }

    @Override
    public long getInt128Low(int position)
    {
        return second;
    }
}
