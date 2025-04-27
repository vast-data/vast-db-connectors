/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

public interface Fixed12BlockApi
{
    long getFixed12First(int position);
    int getFixed12Second(int position);
}
