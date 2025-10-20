/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

public class StartTransactionContext
{
    private final boolean readOnly;
    private final boolean autoCommit;

    public StartTransactionContext(boolean readOnly, boolean autoCommit)
    {
        this.readOnly = readOnly;
        this.autoCommit = autoCommit;
    }

    public boolean isAutoCommit()
    {
        return autoCommit;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }
}
