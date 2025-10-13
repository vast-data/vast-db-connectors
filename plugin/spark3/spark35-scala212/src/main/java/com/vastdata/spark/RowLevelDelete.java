/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE;

public class RowLevelDelete
        extends VastDeltaOperation
{
    public RowLevelDelete(VastTable vastTable)
    {
        super(vastTable);
        vastTable.getTableMD().setForDelete();
    }

    @Override
    public Command command()
    {
        return DELETE;
    }
}
