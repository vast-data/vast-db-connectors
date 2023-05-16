/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import javax.ws.rs.HEAD;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.vastdata.client.importdata.VastImportDataMetadataUtils.getTablePath;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class VastTableHandle
        implements ConnectorTableHandle
{
    private final boolean forImportData;
    private final String schemaName;
    private final String tableName;
    private final boolean updatable; // set if VastUpdatablePageSource should be created (see https://trino.io/docs/current/develop/delete-and-update.html)
    private List<VastColumnHandle> tableColumns;
    private final List<VastColumnHandle> updatedColumns; // set by `VastMetadata#beginUpdate`
    private final Optional<Long> limit;
    private final TupleDomain<VastColumnHandle> predicate; // enforced by connector
    private final Optional<String> bigCatalogSearchPath;
    private final List<VastSubstringMatch> substringMatches; // enforced (i.e. Trino applies post-filtering)

    @JsonCreator
    public VastTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("updatable") boolean updatable,
            @JsonProperty("updatedColumns") List<VastColumnHandle> updatedColumns,
            @JsonProperty("predicate") TupleDomain<VastColumnHandle> predicate,
            @JsonProperty("bigCatalogSearchPath") Optional<String> bigCatalogSearchPath,
            @JsonProperty("substringMatches") List<VastSubstringMatch> substringMatches,
            @JsonProperty("limit") Optional<Long> limit,
            @JsonProperty("forImportData") boolean forImportData)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.updatable = updatable;
        this.updatedColumns = requireNonNull(updatedColumns, "updatedColumns is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.bigCatalogSearchPath = requireNonNull(bigCatalogSearchPath, "bigCatalogSearchPath is null");
        this.substringMatches = requireNonNull(substringMatches, "substringMatches is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.forImportData = forImportData;
    }

    public VastTableHandle(String schemaName, String tableName, boolean forImportData)
    {
        this(schemaName, tableName, false, List.of(), TupleDomain.all(), Optional.empty(), List.of(), Optional.empty(), forImportData);
    }

    public VastTableHandle forDelete()
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName, true, List.of(), predicate, bigCatalogSearchPath, substringMatches, limit, false);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    public VastTableHandle forUpdate(List<VastColumnHandle> updatedColumns)
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName, true, updatedColumns, predicate, bigCatalogSearchPath, substringMatches, limit, false);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    public VastTableHandle withPredicate(TupleDomain<VastColumnHandle> predicate, List<VastSubstringMatch> substringMatches)
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName, updatable, updatedColumns, predicate, bigCatalogSearchPath, substringMatches, limit, false);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    public VastTableHandle withBigCatalogSearchPath(String bigCatalogSearchPath)
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName, updatable, updatedColumns, predicate, Optional.of(bigCatalogSearchPath), substringMatches, limit, false);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    public VastTableHandle withLimit(long limit)
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName, updatable, updatedColumns, predicate, bigCatalogSearchPath, substringMatches, Optional.of(limit), false);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public boolean getUpdatable()
    {
        return updatable;
    }

    @JsonProperty
    public List<VastColumnHandle> getUpdatedColumns()
    {
        return updatedColumns;
    }

    @JsonProperty
    public TupleDomain<VastColumnHandle> getPredicate()
    {
        return predicate;
    }

    @JsonProperty
    public Optional<String> getBigCatalogSearchPath()
    {
        return bigCatalogSearchPath;
    }

    @JsonProperty
    public List<VastSubstringMatch> getSubstringMatches()
    {
        return substringMatches;
    }

    @JsonProperty
    public Optional<Long> getLimit()
    {
        return limit;
    }

    @JsonProperty
    public boolean getForImportData()
    {
        return forImportData;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, updatable, predicate, limit, forImportData);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        VastTableHandle other = (VastTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.updatable, other.updatable) &&
                Objects.equals(this.predicate, other.predicate) &&
                Objects.equals(this.limit, other.limit) &&
                Objects.equals(this.forImportData, other.forImportData);
    }

    @Override
    public String toString()
    {
        return format("%s:%s%s@%s%s", schemaName, tableName, getState(), predicate, limit.map(value -> format(",limit=%d", value)).orElse(""));
    }

    private String getState()
    {
        return updatable ? "(update)" : (forImportData ? "(forImportData)" : "");
    }

    public void clearColumnHandlesCache()
    {
        this.tableColumns = null;
    }

    public void setColumnHandlesCache(List<VastColumnHandle> tableColumns)
    {
        this.tableColumns = tableColumns;
    }

    public List<VastColumnHandle> getColumnHandlesCache()
    {
        return tableColumns;
    }

    public String getPath()
    {
        return getTablePath(this.getSchemaName(), this.getTableName());
    }
}
