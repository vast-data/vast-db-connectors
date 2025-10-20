/* Copyright (C) Vast Data Ltd. */

package com.vastdata.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class VastMergeTableHandle
        implements ConnectorMergeTableHandle
{
    private final VastTableHandle table;
    private final List<VastColumnHandle> columns;

    @JsonCreator
    public VastMergeTableHandle(
            @JsonProperty("table") VastTableHandle table,
            @JsonProperty("columns") List<VastColumnHandle> columns)
    {
        this.table = table;
        this.columns = columns;
    }

    @JsonProperty("table")
    public VastTableHandle getTable()
    {
        return table;
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
        return Objects.equals(table, that.getTable());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table);
    }

    @Override
    public ConnectorTableHandle getTableHandle()
    {
        return this.table;
    }
}
