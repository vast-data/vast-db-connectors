/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.vastdata.trino.predicate.ComplexPredicate;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

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
    private List<VastColumnHandle> tableColumns;
    private final List<VastColumnHandle> mergedColumns; // set by `VastMetadata#beginMerge`
    private final Optional<Long> limit;
    private final TupleDomain<VastColumnHandle> predicate; // enforced by connector
    private final ComplexPredicate complexPredicate;
    private final Optional<String> bigCatalogSearchPath;
    private final List<VastSubstringMatch> substringMatches; // enforced (i.e. Trino applies post-filtering)
    private final String handleID;

    @JsonCreator
    public VastTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("mergedColumns") List<VastColumnHandle> mergedColumns,
            @JsonProperty("predicate") TupleDomain<VastColumnHandle> predicate,
            @JsonProperty("complexPredicate") ComplexPredicate complexPredicate,
            @JsonProperty("bigCatalogSearchPath") Optional<String> bigCatalogSearchPath,
            @JsonProperty("substringMatches") List<VastSubstringMatch> substringMatches,
            @JsonProperty("limit") Optional<Long> limit,
            @JsonProperty("forImportData") boolean forImportData,
            @JsonProperty("handleID") String handleID)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.mergedColumns = requireNonNull(mergedColumns, "mergedColumns is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.complexPredicate = complexPredicate;
        this.bigCatalogSearchPath = requireNonNull(bigCatalogSearchPath, "bigCatalogSearchPath is null");
        this.substringMatches = requireNonNull(substringMatches, "substringMatches is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.forImportData = forImportData;
        this.handleID = requireNonNull(handleID, "handleID is null");
    }

    public VastTableHandle(String schemaName, String tableName, String handleID, boolean forImportData)
    {
        this(schemaName, tableName, List.of(), TupleDomain.all(), null, Optional.empty(), List.of(), Optional.empty(), forImportData, handleID);
    }

    public VastTableHandle forDelete()
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName, List.of(), predicate, complexPredicate, bigCatalogSearchPath, substringMatches, limit, false, handleID);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    public VastTableHandle forMerge(List<VastColumnHandle> mergeableColumns)
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName, mergeableColumns, predicate, complexPredicate, bigCatalogSearchPath, substringMatches, limit, false, handleID);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    public VastTableHandle withPredicate(TupleDomain<VastColumnHandle> predicate, Optional<ComplexPredicate> complexPredicate, List<VastSubstringMatch> substringMatches)
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName, mergedColumns, predicate, complexPredicate.orElse(null), bigCatalogSearchPath, substringMatches, limit, false, handleID);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    public VastTableHandle withBigCatalogSearchPath(String bigCatalogSearchPath)
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName, mergedColumns, predicate, complexPredicate, Optional.of(bigCatalogSearchPath), substringMatches, limit, false, handleID);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    public VastTableHandle withLimit(long limit)
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName, mergedColumns, predicate, complexPredicate, bigCatalogSearchPath, substringMatches, Optional.of(limit), false, handleID);
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
    public List<VastColumnHandle> getMergedColumns()
    {
        return mergedColumns;
    }

    @JsonProperty
    public TupleDomain<VastColumnHandle> getPredicate()
    {
        return predicate;
    }

    @JsonProperty
    public ComplexPredicate getComplexPredicate()
    {
        return complexPredicate;
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
        return Objects.hash(schemaName, tableName, predicate, complexPredicate, substringMatches, limit, forImportData);
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
                Objects.equals(this.predicate, other.predicate) &&
                Objects.equals(this.complexPredicate, other.complexPredicate) &&
                Objects.equals(this.substringMatches, other.substringMatches) &&
                Objects.equals(this.limit, other.limit) &&
                Objects.equals(this.forImportData, other.forImportData);
    }

    @Override
    public String toString()
    {
        return format("%s:%s@%s%s", schemaName, tableName, predicate, limit.map(value -> format(",limit=%d", value)).orElse(""));
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

    @JsonProperty
    public String getHandleID()
    {
        return this.handleID;
    }
}
