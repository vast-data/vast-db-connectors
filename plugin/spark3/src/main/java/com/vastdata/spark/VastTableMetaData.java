/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Objects;

public class VastTableMetaData
        implements Serializable
{
    public final StructType schema;
    public final String schemaName;
    public final String tableName;
    public final String handleID;
    public final boolean forImportData;

    public VastTableMetaData(String schemaName, String tableName, String handleID, StructType schema, boolean forImportData)
    {
        this.schema = schema;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.handleID = handleID;
        this.forImportData = forImportData;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VastTableMetaData that = (VastTableMetaData) o;
        return Objects.equals(schema, that.schema) && Objects.equals(schemaName, that.schemaName) && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schema, schemaName, tableName);
    }
}
