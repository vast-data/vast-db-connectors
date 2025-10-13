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
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext$;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Supplier;

import static org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ;
import static org.apache.spark.sql.connector.catalog.TableCapability.BATCH_WRITE;

public class VastTable
        implements SupportsRead, SupportsWrite, SupportsRowLevelOperations, SupportsReportStatistics
//    TODO - Remove support for SupportsDeleteV2
{
    private static final Logger LOG = LoggerFactory.getLogger(VastTable.class);

    // defaults are set high in order to prevent the Spark compiler making bad decisions such as broadcasting over a table which we do not know it's size
    public static final Integer defaultSizeInBytes = 99999999;
    public static final Integer defaultNumRows = 99999999;
    public static final Statistics DEFAULT_TABLE_LEVEL_STATS = new TableLevelStatistics(OptionalLong.of(defaultSizeInBytes), OptionalLong.of(defaultNumRows), new HashMap<>());
    public static final String HANDLE_ID_PROPERTY = "handleID";
    public static final ImmutableSet<TableCapability> TABLE_CAPABILITIES = ImmutableSet.of(BATCH_READ, BATCH_WRITE);

    private final Supplier<VastClient> clientSupplier;
    private final VastTableMetaData tableMD;
    private final String name;
    private final boolean isPredicatePushdownEnabled;

    public VastTable(String schemaName, String tableName,  String handleID, StructType schema,
                     Supplier<VastClient> clientSupplier, boolean forImportData, boolean isPredicatePushdownEnabled)
    {
        this.tableMD = new VastTableMetaData(schemaName, tableName, handleID, schema, forImportData);
        this.clientSupplier = clientSupplier;
        this.name = schemaName + "/" + tableName;
        this.isPredicatePushdownEnabled = isPredicatePushdownEnabled;
        if (!isPredicatePushdownEnabled) {
            LOG.warn("Predicate pushdown is disabled for table: {}", name);
        }
    }

    public VastTable(String schemaName, String tableName,  String handleID, StructType schema,
                     Supplier<VastClient> clientSupplier, boolean forImportData)
    {
        this(schemaName, tableName, handleID, schema, clientSupplier, forImportData, true);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options)
    {
        VastScanBuilder builder = new VastScanBuilder(this);
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
    public RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info)
    {
        LOG.debug("newRowLevelOperationBuilder({}.{}) {}, {}", tableMD.schemaName, tableMD.tableName, info.command(), info.options().asCaseSensitiveMap());
        return new VastRowLevelOperationBuilder(this, info);
    }

    @Override
    public DeltaWriteBuilder newWriteBuilder(LogicalWriteInfo info)
    {
        LOG.debug("newWriteBuilder({}.{}) {}, {}, {}", tableMD.schemaName, tableMD.tableName, info.queryId(), info.schema(), info.options().asCaseSensitiveMap());
        return new VastWriteBuilder(clientSupplier.get(),this);
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
        LOG.debug("estimateStatistics() is called for table {} in schema {}", this.tableMD.tableName, this.tableMD.schemaName);
        Optional<org.apache.spark.sql.catalyst.plans.logical.Statistics> cachedStats = SparkVastStatisticsManager.getInstance().getTableStatistics(this);
        return cachedStats.map(StatsUtils::sparkCatalystStatsToTableStatistics).orElse(DEFAULT_TABLE_LEVEL_STATS);
    }

    @Override
    public StructType readSchema() {
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
        return new StringJoiner(", ", VastTable.class.getSimpleName() + "[", "]")
                .add("tableMD=" + tableMD)
                .add("name='" + name + "'")
                .add("isPredicatePushdownEnabled=" + isPredicatePushdownEnabled)
                .toString();
    }

    @Override
    public Map<String, String> properties()
    {
        return ImmutableMap.of(HANDLE_ID_PROPERTY, this.tableMD.handleID);
    }

    private static boolean isGlutenEnabled()
    {
        SparkConf conf = SparkContext$.MODULE$.getActive().get().getConf();
        return conf.get("spark.plugins", "").contains("io.glutenproject.GlutenPlugin");
    }
}
