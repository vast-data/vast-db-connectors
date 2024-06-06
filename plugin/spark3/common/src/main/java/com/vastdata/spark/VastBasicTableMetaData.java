/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import java.io.Serializable;
import java.util.Objects;

public class VastBasicTableMetaData
        implements Serializable
{
    public final String schemaName;
    public final String tableName;
    public final String handleID;
    public final boolean forImportData;

    public VastBasicTableMetaData(String schemaName, String tableName, String handleID, boolean forImportData)
    {
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
        VastBasicTableMetaData that = (VastBasicTableMetaData) o;
        return Objects.equals(schemaName, that.schemaName) && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName);
    }
}
