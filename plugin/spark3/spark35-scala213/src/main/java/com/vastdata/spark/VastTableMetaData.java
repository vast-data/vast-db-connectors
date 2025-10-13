/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.spark.sql.types.StructType;

import java.util.Objects;

public class VastTableMetaData
        extends VastBasicTableMetaData
{
    public final StructType schema;
    private boolean forUpdate = false;
    private boolean forDelete = false;
    public VastTableMetaData(String schemaName, String tableName, String handleID, StructType schema, boolean forImportData)
    {
        super(schemaName, tableName, handleID, forImportData);
        this.schema = schema;
    }

    public void setForUpdate()
    {
        forUpdate = true;
    }

    public boolean isForUpdate()
    {
        return forUpdate;
    }

    public void setForDelete()
    {
        forDelete = true;
    }

    public boolean isForDelete()
    {
        return forDelete;
    }

    @Override
    public boolean equals(Object o)
    {
        if (super.equals(o)) {
            VastTableMetaData that = (VastTableMetaData) o;
            return Objects.equals(schema, that.schema) && Objects.equals(schemaName, that.schemaName);
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schema, schemaName, tableName);
    }
}
