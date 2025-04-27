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
    public VastTableMetaData(String schemaName, String tableName, String handleID, StructType schema, boolean forImportData)
    {
        super(schemaName, tableName, handleID, forImportData);
        this.schema = schema;
    }

    @Override
    public boolean equals(Object o)
    {
        return super.equals(o) && Objects.equals(schema, ((VastTableMetaData) o).schema);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schema, schemaName, tableName);
    }
}
