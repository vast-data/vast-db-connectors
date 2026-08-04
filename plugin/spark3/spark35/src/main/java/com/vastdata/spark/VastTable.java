/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.vastdata.client.VastClient;
import com.vastdata.spark.statistics.SparkVastStatisticsManager;
import com.vastdata.spark.statistics.StatsUtils;
import com.vastdata.spark.statistics.TableLevelStatistics;
import com.vastdata.spark.write.VastWriteBuilder;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.sql.catalog.ndb.VastCatalogUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Supplier;

import static com.vastdata.client.schema.VastMetadataUtils.SORTED_BY_PROPERTY;
import static org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ;
import static org.apache.spark.sql.connector.catalog.TableCapability.BATCH_WRITE;

public class VastTable
        implements SupportsRead, SupportsWrite, SupportsRowLevelOperations,
        SupportsReportStatistics
        //    TODO - Remove support for SupportsDeleteV2
{
    // defaults are set high in order to prevent the Spark compiler making bad decisions such as broadcasting over a table which we do not know it's size
    public static final Integer defaultSizeInBytes = 99999999;
    public static final Integer defaultNumRows = 99999999;
    public static final Statistics DEFAULT_TABLE_LEVEL_STATS = new TableLevelStatistics(
            OptionalLong.of(defaultSizeInBytes),
            OptionalLong.of(defaultNumRows), new HashMap<>());
    public static final String HANDLE_ID_PROPERTY = "handleID";
    public static final ImmutableSet<TableCapability> TABLE_CAPABILITIES = ImmutableSet.of(
            BATCH_READ, BATCH_WRITE);
    private static final Logger LOG = LoggerFactory.getLogger(VastTable.class);
    protected final Map<String, String> additionalProperties;
    protected final Supplier<VastClient> clientSupplier;
    protected final VastTableMetaData tableMD;
    protected final VastCatalogUtils vastCatalogUtils;
    protected final Optional<RuntimeException> notSafeToWrite;
    private final Transform[] partitioning;
    private final String name;
    private final boolean isPredicatePushdownEnabled;

    public VastTable(VastCatalogUtils vastCatalogUtils, String schemaName,
            String tableName, String handleID, StructType schema,
            Transform[] partitioning, Supplier<VastClient> clientSupplier,
            boolean forImportData, boolean isPredicatePushdownEnabled,
            Optional<RuntimeException> notSafeToWrite,
            Map<String, String> additionalProperties)
    {
        this.tableMD = new VastTableMetaData(schemaName, tableName, handleID,
                schema, forImportData);
        this.partitioning = partitioning;
        this.clientSupplier = clientSupplier;
        this.name = schemaName + "/" + tableName;
        this.isPredicatePushdownEnabled = isPredicatePushdownEnabled;
        this.additionalProperties = additionalProperties;
        this.vastCatalogUtils = vastCatalogUtils;
        this.notSafeToWrite = notSafeToWrite;
        if (!isPredicatePushdownEnabled) {
            LOG.warn("Predicate pushdown is disabled for table: {}", name);
        }
    }

    public VastTable(VastCatalogUtils vastCatalogUtils, String schemaName,
            String tableName, String handleID, StructType schema,
            Transform[] partitioning, Supplier<VastClient> clientSupplier,
            boolean forImportData, Optional<RuntimeException> notSafeToWrite,
            Map<String, String> additionalProperties)
    {
        this(vastCatalogUtils, schemaName, tableName, handleID, schema,
                partitioning, clientSupplier, forImportData, true,
                notSafeToWrite, additionalProperties);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options)
    {
        VastScanBuilder builder = new VastScanBuilder(this, vastCatalogUtils);
        if (!isPredicatePushdownEnabled) {
            builder.disablePredicatePushdown();
        }
        return builder;
    }

    @Override
    public String name()
    {
        return this.name;
    }

    @Override
    public StructType schema()
    {
        return tableMD.schema;
    }

    @Override
    public Set<TableCapability> capabilities()
    {
        return TABLE_CAPABILITIES;
    }

    @Override
    public RowLevelOperationBuilder newRowLevelOperationBuilder(
            RowLevelOperationInfo info)
    {
        LOG.debug("newRowLevelOperationBuilder({}.{}) {}, {}",
                tableMD.schemaName, tableMD.tableName, info.command(),
                info.options().asCaseSensitiveMap());
        return new VastRowLevelOperationBuilder(this, info);
    }

    @Override
    public DeltaWriteBuilder newWriteBuilder(LogicalWriteInfo info)
    {
        if (!notSafeToWrite.isPresent()) {
            LOG.debug("newWriteBuilder({}.{}) {}, {}, {}", tableMD.schemaName,
                    tableMD.tableName, info.queryId(), info.schema(),
                    info.options().asCaseSensitiveMap());
            return new VastWriteBuilder(clientSupplier.get(), this);
        }
        else {
            final RuntimeException error = new RuntimeException(
                    notSafeToWrite.get().getMessage(), notSafeToWrite.get());
            LOG.error("Write attempt with an unsafe Spark configuration",
                    error);
            throw error;
        }
    }

    public String getSchemaName()
    {
        return tableMD.schemaName;
    }

    public VastTableMetaData getTableMD()
    {
        return tableMD;
    }

    public String getTableHandleID()
    {
        return this.tableMD.handleID;
    }

    @Override
    public Statistics estimateStatistics()
    {
        LOG.debug("estimateStatistics() is called for table {} in schema {}",
                this.tableMD.tableName, this.tableMD.schemaName);
        Optional<org.apache.spark.sql.catalyst.plans.logical.Statistics> cachedStats = SparkVastStatisticsManager
                .getInstance()
                .getTableStatistics(this);
        return cachedStats
                .map(StatsUtils::sparkCatalystStatsToTableStatistics)
                .orElse(DEFAULT_TABLE_LEVEL_STATS);
    }

    @Override
    public StructType readSchema()
    {
        return this.schema();
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
        VastTable vastTable = (VastTable) o;
        return Objects.equals(getTableMD(), vastTable.getTableMD());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getTableMD());
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", VastTable.class.getSimpleName() + "[",
                "]")
                .add("tableMD=" + tableMD)
                .add("name='" + name + "'")
                .add("isPredicatePushdownEnabled=" + isPredicatePushdownEnabled)
                .toString();
    }

    @Override
    public Map<String, String> properties()
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.putAll(additionalProperties);
        builder.put(HANDLE_ID_PROPERTY, this.tableMD.handleID);
        return builder.build();
    }

    @Override
    public Transform[] partitioning()
    {
        return partitioning;
    }

    public Set<String> getNonUpdatableColumns()
    {
        String sortedByProp = properties().get(SORTED_BY_PROPERTY);
        Set<String> nonUpdateCols = (sortedByProp != null && !sortedByProp.isEmpty())
                ? new HashSet<>(Arrays.asList(sortedByProp.split(",")))
                : new HashSet<>();
        if (partitioning != null) {
            for (Transform t : partitioning) {
                nonUpdateCols.add(t.references()[0].fieldNames()[0]);
            }
        }
        return nonUpdateCols;
    }
}
