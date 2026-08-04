/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.vastdata.client.QueryDataExtraParams;
import com.vastdata.client.TableSpecifiers;
import com.vastdata.client.VastObjectDetails;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.partition.PartitionColumnMetadata;
import com.vastdata.client.rowid.RowIDStrategyType;
import com.vastdata.trino.predicate.ComplexPredicate;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.partition.PartitionConstants.PIT_NAME_SUFFIX;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class VastTableHandle
        implements ConnectorTableHandle
{
    private final boolean forImportData;
    private final boolean forNonAcidOperation;
    private final String schemaName;
    private final String tableName;
    private final List<VastColumnHandle> mergedColumns; // set by `VastMetadata#beginMerge`
    private final Optional<List<String>> sortedColumns;
    private final Optional<Set<VastColumnHandle>> analyzeColumns;
    private final Optional<List<PartitionColumnMetadata>> partitionColumns;
    private final Optional<Long> limit;
    private final TupleDomain<VastColumnHandle> predicate; // enforced by connector
    private final ComplexPredicate complexPredicate;
    private final Optional<String> bigCatalogSearchPath;
    private final List<VastSubstringMatch> substringMatches; // enforced (i.e. Trino applies post-filtering)
    private final VastObjectDetails handleID;
    private final QueryDataExtraParams extraQueryParams;
    private List<VastColumnHandle> tableColumns;
    private transient List<String> partitionColumnNamesCache;

    @JsonCreator
    public VastTableHandle(@JsonProperty("schemaName") String schemaName,
                           @JsonProperty("tableName") String tableName,
                           @JsonProperty("mergedColumns") List<VastColumnHandle> mergedColumns,
                           @JsonProperty("sortedColumns") Optional<List<String>> sortedColumns,
                           @JsonProperty("partitionColumns") Optional<List<PartitionColumnMetadata>> partitionColumns,
                           @JsonProperty("predicate") TupleDomain<VastColumnHandle> predicate,
                           @JsonProperty("complexPredicate") ComplexPredicate complexPredicate,
                           @JsonProperty("bigCatalogSearchPath") Optional<String> bigCatalogSearchPath,
                           @JsonProperty("substringMatches") List<VastSubstringMatch> substringMatches,
                           @JsonProperty("analyzeColumns") Optional<Set<VastColumnHandle>> analyzeColumns,
                           @JsonProperty("limit") Optional<Long> limit,
                           @JsonProperty("extraQueryParams") QueryDataExtraParams extraQueryParams,
                           @JsonProperty("forImportData") boolean forImportData,
                           @JsonProperty("forNonAcidOperation") boolean forNonAcidOperation,
                           @JsonProperty("handleID") VastObjectDetails handleID)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.mergedColumns = requireNonNull(mergedColumns,
                "mergedColumns is null");
        this.sortedColumns = requireNonNull(sortedColumns,
                "sortedColumns is null");

        this.partitionColumns = requireNonNull(partitionColumns,
                "partitionColumns is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.complexPredicate = complexPredicate;
        this.bigCatalogSearchPath = requireNonNull(bigCatalogSearchPath,
                "bigCatalogSearchPath is null");
        this.substringMatches = requireNonNull(substringMatches,
                "substringMatches is null");
        this.analyzeColumns = requireNonNull(analyzeColumns, "sortedColumns is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.extraQueryParams = requireNonNull(extraQueryParams,
                "extraQueryParams is null");
        this.forImportData = forImportData;
        this.forNonAcidOperation = forNonAcidOperation;
        this.handleID = requireNonNull(handleID, "handleID is null");
    }

    public VastTableHandle(String schemaName,
                           String tableName,
                           VastObjectDetails handleID,
                           boolean forImportData,
                           boolean forNonAcidOperation)
    {
        this(schemaName, tableName, List.of(), Optional.empty(),
                Optional.empty(), TupleDomain.all(), null, Optional.empty(),
                List.of(), Optional.empty(), Optional.empty(),
                new QueryDataExtraParams(), forImportData, forNonAcidOperation,
                handleID);
    }

    public VastTableHandle forDelete()
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName,
                List.of(), sortedColumns, partitionColumns, predicate,
                complexPredicate, bigCatalogSearchPath, substringMatches,
                analyzeColumns, limit, extraQueryParams, false,
                forNonAcidOperation, handleID);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    public VastTableHandle forMerge(List<VastColumnHandle> mergeableColumns)
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName,
                mergeableColumns, sortedColumns, partitionColumns, predicate,
                complexPredicate, bigCatalogSearchPath, substringMatches,
                analyzeColumns, limit, extraQueryParams, false,
                forNonAcidOperation, handleID);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    public VastTableHandle withPredicate(TupleDomain<VastColumnHandle> predicate,
                                         Optional<ComplexPredicate> complexPredicate,
                                         List<VastSubstringMatch> substringMatches)
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName,
                mergedColumns, sortedColumns, partitionColumns, predicate,
                complexPredicate.orElse(null), bigCatalogSearchPath,
                substringMatches, analyzeColumns, limit, extraQueryParams,
                false, forNonAcidOperation, handleID);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    public VastTableHandle withBigCatalogSearchPath(String bigCatalogSearchPath)
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName,
                List.of(), sortedColumns, partitionColumns, predicate,
                complexPredicate, Optional.of(bigCatalogSearchPath),
                substringMatches, analyzeColumns, limit, extraQueryParams,
                false, forNonAcidOperation, handleID);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    public VastTableHandle withLimit(long limit)
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName,
                List.of(), sortedColumns, partitionColumns, predicate,
                complexPredicate, bigCatalogSearchPath, substringMatches,
                analyzeColumns, Optional.of(limit), extraQueryParams, false,
                forNonAcidOperation, handleID);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    public VastTableHandle withSortedColumns(List<String> sortedColumns)
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName,
                mergedColumns, Optional.ofNullable(sortedColumns),
                partitionColumns, predicate, complexPredicate,
                bigCatalogSearchPath, substringMatches, analyzeColumns, limit,
                extraQueryParams, forImportData, forNonAcidOperation, handleID);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    public VastTableHandle withPartitionColumns(List<PartitionColumnMetadata> partitionColumns)
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName,
                mergedColumns, sortedColumns,
                Optional.ofNullable(partitionColumns), predicate,
                complexPredicate, bigCatalogSearchPath, substringMatches,
                analyzeColumns, limit, extraQueryParams, forImportData,
                forNonAcidOperation, handleID);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    public VastTableHandle withAnalyzeColumns(Set<VastColumnHandle> analyzeColumns)
    {
        VastTableHandle newHandle = new VastTableHandle(schemaName, tableName,
                mergedColumns, sortedColumns, partitionColumns, predicate,
                complexPredicate, bigCatalogSearchPath, substringMatches,
                Optional.ofNullable(analyzeColumns), limit, extraQueryParams,
                forImportData, forNonAcidOperation, handleID);
        newHandle.setColumnHandlesCache(this.tableColumns);
        return newHandle;
    }

    public void addExtraQueryParams(QueryDataExtraParams.QueryDataExtraParamType type, String key, String value)
    {
        extraQueryParams.addExtraQueryParams(type, key, value);
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

    @JsonProperty
    public boolean getForNonAcidOperation()
    {
        return forNonAcidOperation;
    }

    public boolean isPit()
    {
        return tableName.endsWith(PIT_NAME_SUFFIX);
    }

    @JsonProperty
    public Optional<List<String>> getSortedColumns()
    {
        return sortedColumns;
    }

    @JsonProperty
    public Optional<List<PartitionColumnMetadata>> getPartitionColumns()
    {
        return partitionColumns;
    }

    @JsonProperty
    public Optional<Set<VastColumnHandle>> getAnalyzeColumns()
    {
        return analyzeColumns;
    }

    public Optional<List<String>> getPartitionPostTransformColumnNames()
    {
        if (partitionColumns.isPresent()) {
            if (partitionColumnNamesCache == null) {
                partitionColumnNamesCache = partitionColumns
                        .orElseThrow()
                        .stream()
                        .map(PartitionColumnMetadata::getColumnName)
                        .toList();
            }
            return partitionColumnNamesCache.isEmpty() ?
                    Optional.empty() :
                    Optional.of(partitionColumnNamesCache);
        }
        else {
            return Optional.empty();
        }
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, predicate, complexPredicate,
                substringMatches, limit, forImportData, forNonAcidOperation);
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
        return Objects.equals(this.schemaName,
                other.schemaName) && Objects.equals(this.tableName,
                other.tableName) && Objects.equals(this.predicate,
                other.predicate) && Objects.equals(this.complexPredicate,
                other.complexPredicate) && Objects.equals(this.substringMatches,
                other.substringMatches) && Objects.equals(this.limit,
                other.limit) && Objects.equals(this.forImportData,
                other.forImportData) && Objects.equals(this.forNonAcidOperation,
                other.forNonAcidOperation);
    }

    @Override
    public String toString()
    {
        return format("%s:%s@%s:complex=%s:limit=%s:sorted_by@[%s], handle:%s", schemaName,
                tableName, predicate, complexPredicate,
                limit.map(value -> format("%d", value)).orElse("none"),
                getSortedColumns(), handleID);
    }

    public void clearColumnHandlesCache()
    {
        this.tableColumns = null;
    }

    public void requireNonAcidOperationAllowed()
    {
        if (!getForNonAcidOperation()) {
            throw toRuntime(new VastUserException(
                    "operation is non-acid operation you must add the vast.allow_non_acid flag"));
        }
    }

    public List<VastColumnHandle> getColumnHandlesCache()
    {
        return tableColumns;
    }

    public void setColumnHandlesCache(List<VastColumnHandle> tableColumns)
    {
        this.tableColumns = tableColumns;
    }

    public String getPath()
    {
        return format("/%s/%s", this.getSchemaName(),
                TableSpecifiers.parse(this.getTableName()).getTableName());
    }

    @JsonProperty
    public VastObjectDetails getHandleID()
    {
        return this.handleID;
    }

    public Optional<VastPartitioningHandle> getPartitioningHandle()
    {
        return partitionColumns.map(
                columns -> VastPartitioningHandle.create(columns,
                        tableColumns));
    }

    private Set<String> getNonUpdateableColumns()
    {
        return Stream
                .concat(
                partitionColumns.stream()
                .flatMap(List::stream).map(PartitionColumnMetadata::getSourceColumnName),
                getSortedColumns().stream()
                .flatMap(List::stream)
                ).collect(Collectors.toSet());
    }

    public Predicate<String> getIsNonUpdateableColumnPredicate()
    {
        return columnName -> this
                .getNonUpdateableColumns()
                .contains(columnName.toLowerCase(Locale.ENGLISH));
    }

    public RowIDStrategyType getRowIdStrategyType()
    {
        if (getSortedColumns().isPresent() || getPartitionColumns().isPresent()) {
            return RowIDStrategyType.DECIMAL_128;
        }
        return RowIDStrategyType.UNSIGNED_INT64;
    }

    @JsonProperty
    public QueryDataExtraParams getExtraQueryParams()
    {
        return extraQueryParams;
    }
}
