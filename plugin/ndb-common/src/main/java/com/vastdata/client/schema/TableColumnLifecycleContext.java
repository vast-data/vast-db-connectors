/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.StringJoiner;

public class TableColumnLifecycleContext
{
    private final Field field;
    private final String tableName;
    private final String schemaName;

    public TableColumnLifecycleContext(String schemaName, String tableName, Field field)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.field = field;
    }

    public Field getField()
    {
        return this.field;
    }

    public String getTableName()
    {
        return this.tableName;
    }

    public String getSchemaName()
    {
        return this.schemaName;
    }

    public String getColumnName()
    {
        return this.field.getName();
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", TableColumnLifecycleContext.class.getSimpleName() + "[", "]")
                .add("field=" + field)
                .add("tableName='" + tableName + "'")
                .add("schemaName='" + schemaName + "'")
                .toString();
    }
}
