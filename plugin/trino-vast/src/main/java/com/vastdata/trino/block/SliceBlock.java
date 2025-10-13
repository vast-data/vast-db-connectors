/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

import io.airlift.slice.Slice;

public interface SliceBlock
{
    Slice getSlice(int position);
    int getSliceLength(int position);
}
