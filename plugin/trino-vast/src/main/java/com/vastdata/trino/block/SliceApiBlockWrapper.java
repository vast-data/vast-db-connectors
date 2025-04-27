/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

import io.airlift.slice.Slice;

import java.util.function.Function;

class SliceApiBlockWrapper
        implements SliceBlock
{
    private final Function<Integer, Slice> sliceFunction;
    private final Function<Integer, Integer> sliceLengthFunction;


    SliceApiBlockWrapper(Function<Integer, Slice> sliceFunction, Function<Integer, Integer> sliceLengthFunction) {
        this.sliceFunction = sliceFunction;
        this.sliceLengthFunction = sliceLengthFunction;
    }

    @Override
    public Slice getSlice(int position)
    {
        return sliceFunction.apply(position);
    }

    @Override
    public int getSliceLength(int position)
    {
        return sliceLengthFunction.apply(position);
    }
}
