/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

public interface Int128ArrayBlockApi
{
    long getInt128High(int position);
    long getInt128Low(int position);
}
