/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata;

public interface ValueEntrySetter<T>
{
    void setNull(int i);

    void set(int i, T val);
}
