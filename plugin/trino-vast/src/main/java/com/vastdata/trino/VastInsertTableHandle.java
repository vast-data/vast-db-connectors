/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class VastInsertTableHandle
        implements ConnectorInsertTableHandle, ConnectorOutputTableHandle
{
    private final VastTableHandle table;
    private final List<VastColumnHandle> columns;
    private final boolean create;

    private final boolean forImportData;

    @JsonCreator
    public VastInsertTableHandle(
            @JsonProperty("table") VastTableHandle table,
            @JsonProperty("columns") List<VastColumnHandle> columns,
            @JsonProperty("create") boolean create,
            @JsonProperty("forImportData") boolean forImportData)
    {
        this.table = table;
        this.columns = columns;
        this.create = create;
        this.forImportData = forImportData;
    }

    @JsonProperty("table")
    public VastTableHandle getTable()
    {
        return table;
    }

    @JsonProperty("forImportData")
    public boolean isForImportData()
    {
        return forImportData;
    }

    @JsonProperty("create")
    public boolean isCreate()
    {
        return create;
    }

    @JsonProperty("columns")
    public List<VastColumnHandle> getColumns()
    {
        return columns;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("create", create)
                .toString();
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
        VastInsertTableHandle that = (VastInsertTableHandle) o;
        return create == that.create && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, create);
    }
}
