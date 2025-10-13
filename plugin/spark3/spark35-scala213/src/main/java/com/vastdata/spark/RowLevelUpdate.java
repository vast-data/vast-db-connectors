/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

public class RowLevelUpdate
        extends VastDeltaOperation
{
    public RowLevelUpdate(VastTable vastTable) {
        super(vastTable);
        vastTable.getTableMD().setForUpdate();
    }

    @Override
    public Command command()
    {
        return Command.UPDATE;
    }
}
