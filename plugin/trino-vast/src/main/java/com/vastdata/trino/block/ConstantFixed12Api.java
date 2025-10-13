/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

class ConstantFixed12Api implements Fixed12BlockApi
{
    private final long first;
    private final int second;

    ConstantFixed12Api(long first, int second) {
        this.second = second;
        this.first = first;
    }

    @Override
    public long getFixed12First(int position)
    {
        return first;
    }

    @Override
    public int getFixed12Second(int position)
    {
        return second;
    }
}
