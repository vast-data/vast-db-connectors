/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata;

public interface ValueEntryGetter<T>
{
    boolean isNull(int i);

    boolean isParentNull(int i);

    T get(int i);
}
