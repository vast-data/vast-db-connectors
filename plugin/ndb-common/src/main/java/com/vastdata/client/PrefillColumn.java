/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client;

public class PrefillColumn<C>
{
    private final int projectionIndex;
    private final C columnHandle;
    private final Object defaultValue;

    public PrefillColumn(int projectionIndex, C columnHandle,
            Object defaultValue)
    {
        this.projectionIndex = projectionIndex;
        this.columnHandle = columnHandle;
        this.defaultValue = defaultValue;
    }

    public int getProjectionIndex()
    {
        return projectionIndex;
    }

    public C getColumnHandle()
    {
        return columnHandle;
    }

    public Object getDefaultValue()
    {
        return defaultValue;
    }
}
