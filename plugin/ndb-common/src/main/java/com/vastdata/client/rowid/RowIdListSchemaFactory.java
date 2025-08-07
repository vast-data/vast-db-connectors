/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.rowid;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class RowIdListSchemaFactory
{
    private RowIdListSchemaFactory() {}

    public static Schema get(TableType tableType)
    {
        RowIDStrategy rowIDStrategy = RowIDStrategyFactory.fromTableType(tableType);
        Field rowIdField = rowIDStrategy.get();
        return new Schema(ImmutableList.of(rowIdField));
    }
}
