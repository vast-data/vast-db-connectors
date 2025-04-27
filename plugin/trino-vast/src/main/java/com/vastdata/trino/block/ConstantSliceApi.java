/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

import io.airlift.slice.Slice;

class ConstantSliceApi
        implements SliceBlock
{
    private final Slice slice;

    ConstantSliceApi(Slice slice) {
        this.slice = slice;
    }

    @Override
    public Slice getSlice(int position)
    {
        return slice;
    }

    @Override
    public int getSliceLength(int position)
    {
        return slice.length();
    }
}
