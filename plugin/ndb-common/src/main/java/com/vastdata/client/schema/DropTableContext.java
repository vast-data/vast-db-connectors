/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

public class DropTableContext
{
    private final String schemaName;

    private final String tableName;

    public DropTableContext(String schemaName, String tableName)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }
}
