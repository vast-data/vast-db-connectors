/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.statistics;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.trino.TypeUtils;
import com.vastdata.trino.VastColumnHandle;
import com.vastdata.trino.VastTableHandle;
import com.vastdata.trino.VastTrinoConfig;
import io.airlift.log.Logger;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatisticType;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.vastdata.trino.statistics.VastColumnStatisticType.MAX_VALUE;
import static com.vastdata.trino.statistics.VastColumnStatisticType.MIN_VALUE;
import static com.vastdata.trino.statistics.VastColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.vastdata.trino.statistics.VastColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static com.vastdata.trino.statistics.VastColumnStatisticType.TOTAL_SIZE_IN_BYTES;
import static io.trino.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalConversions.longDecimalToDouble;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;

@SuppressWarnings("deprecation")
public class VastStatisticsManager
{
    private static final Logger LOG = Logger.get(VastStatisticsManager.class);
    private static final BiFunction<Field, Block, Optional<Double>> minMaxValueExtractorForColumnStatistics = (field, block) ->
    {
        int positionCount = block.getPositionCount();
        if (positionCount == 0) {
            return Optional.empty();
        }
        if (block.isNull(0)) {
            return Optional.empty();
        }
        Type type = TypeUtils.convertArrowFieldToTrinoType(field);
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(
                SMALLINT) || type.equals(TINYINT)) {
            return Optional.of(((Long) type.getLong(block, 0)).doubleValue());
        }
        if (type.equals(DOUBLE)) {
            return Optional.of(type.getDouble(block, 0));
        }
        else if (type.equals(REAL)) {
            IntArrayBlock intArrayBlock = (IntArrayBlock) block;
            return Optional.of(((Float) intBitsToFloat(
                    intArrayBlock.getInt(0))).doubleValue());
        }
        else if (type.equals(BOOLEAN)) {
            ByteArrayBlock byteArrayBlock = (ByteArrayBlock) block;
            return Optional.of(
                    ((Byte) byteArrayBlock.getByte(0)).doubleValue());
        }
        else if (type.equals(
                VARCHAR) || type instanceof CharType || type == VARBINARY || type == UUID) {
            return Optional.empty(); // Trino does not support non-numeric min/max values
        }
        if (type instanceof DecimalType decimalType) {
            Int128 int128 = Int128.valueOf(Decimals
                    .readBigDecimal(decimalType, block, 0)
                    .unscaledValue());
            return Optional.of(
                    longDecimalToDouble(int128, decimalType.getScale()));
        }
        if (type.equals(DATE)) {
            IntArrayBlock integerArrayBlock = (IntArrayBlock) block;
            return Optional.of(
                    ((Integer) integerArrayBlock.getInt(0)).doubleValue());
        }
        if (type instanceof TimestampType ts) {
            TimeUnit timeUnit = TypeUtils.precisionToTimeUnit(
                    ts.getPrecision());
            if (timeUnit == TimeUnit.NANOSECOND) {
                return Optional.empty(); // Trino converts long to double which is lossy for timestamp(9)
            }
            LongArrayBlock longArrayBlock = (LongArrayBlock) block;
            return Optional.of(
                    ((Long) longArrayBlock.getLong(0)).doubleValue());
        }
        if (type instanceof TimestampWithTimeZoneType ts) {
            TimeUnit timeUnit = TypeUtils.precisionToTimeUnit(
                    ts.getPrecision());
            if (timeUnit == TimeUnit.NANOSECOND || timeUnit == TimeUnit.MICROSECOND) {
                return Optional.empty(); // Trino doesn't support min/max values for these precisions
            }
            LongArrayBlock longArrayBlock = (LongArrayBlock) block;
            return Optional.of(((Long) unpackMillisUtc(
                    longArrayBlock.getLong(0))).doubleValue());
        }
        if (type instanceof TimeType) {
            return Optional.empty(); // Trino does not support displaying time types in show stats
        }
        throw new UnsupportedOperationException(
                "Unsupported Trino type for column statistics: " + type);
    };
    private static final BiFunction<Field, Block, Optional<Double>> longValExtractor = (field, block) ->
    {
        if (block.getPositionCount() == 0 || block.isNull(0)) {
            return Optional.empty();
        }
        LongArrayBlock longArrayBlock = (LongArrayBlock) block;
        return Optional.of(((Long) longArrayBlock.getLong(0)).doubleValue());
    };
    private static final Map<ColumnStatisticType, BiFunction<Field, Block, Optional<Double>>> supportedStatisticsValueExtractors = Map.of(
            ColumnStatisticType.MIN_VALUE,
            minMaxValueExtractorForColumnStatistics,
            ColumnStatisticType.MAX_VALUE,
            minMaxValueExtractorForColumnStatistics,
            ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES, longValExtractor,
            ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES, longValExtractor,
            ColumnStatisticType.TOTAL_SIZE_IN_BYTES, longValExtractor);
    private final TrinoPersistentStatistics storage;

    @Inject
    public VastStatisticsManager(final VastClient client,
                                 final VastConfig config,
                                 final VastTrinoConfig trinoConfig)
    {
        this.storage = new TrinoPersistentStatistics(client, config,
                trinoConfig);
    }

    private static BiFunction<Field, Block, Optional<Double>> getUnsupportedStatisticsTypeThrower(
            ColumnStatisticType type)
    {
        return (field, block) ->
        {
            throw new UnsupportedOperationException(
                    format("Unsupported statistics type: %s", type));
        };
    }

    public static Set<VastColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        if (type.equals(BOOLEAN) || isNumericType(type) || type.equals(
                DATE) || type instanceof TimeType || type instanceof TimestampType || type instanceof TimestampWithTimeZoneType) {
            return ImmutableSet.of(MIN_VALUE, MAX_VALUE,
                    NUMBER_OF_DISTINCT_VALUES, NUMBER_OF_NON_NULL_VALUES);
        }
        if (type instanceof VarcharType || type instanceof CharType || type.equals(
                VARBINARY)) {
            return ImmutableSet.of(MIN_VALUE, MAX_VALUE,
                    NUMBER_OF_DISTINCT_VALUES, NUMBER_OF_NON_NULL_VALUES,
                    TOTAL_SIZE_IN_BYTES);
        }
        if (type instanceof ArrayType || type instanceof RowType || type instanceof UuidType || type instanceof MapType) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES,
                    NUMBER_OF_DISTINCT_VALUES, TOTAL_SIZE_IN_BYTES);
        }
        LOG.warn("Unsupported column type: %s. using default stats", type);
        return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES,
                NUMBER_OF_DISTINCT_VALUES, TOTAL_SIZE_IN_BYTES);
    }

    private static boolean isNumericType(Type type)
    {
        return type.equals(BIGINT) || type.equals(INTEGER) || type.equals(
                SMALLINT) || type.equals(TINYINT) || type.equals(
                DOUBLE) || type.equals(REAL) || type instanceof DecimalType;
    }

    public Optional<TableStatistics> getTableStatistics(VastTableHandle table)
    {
        try {
            return this.storage.getTableStatistics(table);
        }
        catch (Exception e) {
            LOG.info("Failed to get table statistics file for %s:\n %s",
                    table.toSchemaTableName(), e);
            return Optional.empty();
        }
    }

    public TableStatisticsMetadata getTableStatisticsMetadata(Stream<VastColumnHandle> columns)
    {
        Set<TableStatisticType> tableStatistics = Set.of(
                TableStatisticType.ROW_COUNT);
        Set<ColumnStatisticMetadata> columnStatistics = columns
                .map(this::getColumnStatisticMetadata)
                .flatMap(List::stream)
                .collect(toImmutableSet());

        return new TableStatisticsMetadata(columnStatistics, tableStatistics,
                List.of());
    }

    public void applyTableStatistics(VastTableHandle table,
                                     List<VastColumnHandle> tableColumnFields,
                                     ComputedStatistics statistics)
    {
        long rowCount = getTableRowCount(statistics.getTableStatistics());
        final Map<ColumnHandle, ColumnStatistics> transformedColumnStatistics = transformColumnsStatistics(
                table, rowCount, tableColumnFields,
                statistics.getColumnStatistics());
        Optional<TableStatistics> storageTableStatistics = storage.getTableStatistics(
                table);
        if (storageTableStatistics.isPresent()) {
            TableStatistics existingStatistics = storageTableStatistics.orElseThrow();

            Map<ColumnHandle, ColumnStatistics> mergedColumnStatistics = new HashMap<>(
                    existingStatistics.getColumnStatistics());
            Optional<Set<VastColumnHandle>> analyzeColumns = table.getAnalyzeColumns();
            analyzeColumns.ifPresent(
                    vastColumnHandles -> vastColumnHandles.forEach(
                            vch -> mergedColumnStatistics.put(vch,
                                    transformedColumnStatistics.get(vch))));
            this.storage.setTableStatistics(table,
                    new TableStatistics(Estimate.of(rowCount),
                            mergedColumnStatistics));
        }
        else {
            this.storage.setTableStatistics(table,
                    new TableStatistics(Estimate.of(rowCount),
                            transformedColumnStatistics));
        }
    }

    private long getTableRowCount(Map<TableStatisticType, Block> tableStatistics)
    {
        Block rowCountBlock = Optional
                .ofNullable(tableStatistics.get(ROW_COUNT))
                .orElseThrow(() -> new VerifyException(
                        "Row count table statistic not present"));
        verify(!rowCountBlock.isNull(0), "Row count table statistic is null");
        return BIGINT.getLong(rowCountBlock, 0);
    }

    private Map<ColumnHandle, ColumnStatistics> transformColumnsStatistics(
            VastTableHandle table,
            long rowCount,
            List<VastColumnHandle> tableColumnFields,
            Map<ColumnStatisticMetadata, Block> columnStatistics)
    {
        List<String> partitionColumns = table
                .getPartitionPostTransformColumnNames()
                .orElse(Collections.emptyList());
        return tableColumnFields
                .stream()
                .collect(Collectors.toMap(Function.identity(), vch ->
                {
                    String fieldName = vch.getField().getName();
                    LOG.info("TRANSFORM of column %s", fieldName);
                    PerColumnStatsBuilder perColumnStatsBuilder = new PerColumnStatsBuilder(
                            rowCount);
                    columnStatistics
                            .entrySet()
                            .stream()
                            .filter(columnStatsMapEntry -> columnStatsMapEntry
                                    .getKey()
                                    .getColumnName()
                                    .equals(fieldName))
                            .forEach(columnStatsMapEntry ->
                            {
                                ColumnStatisticType statisticType = ColumnStatisticType.valueOf(
                                        columnStatsMapEntry
                                                .getKey()
                                                .getConnectorAggregationId());
                                Optional<Double> optionalDouble = supportedStatisticsValueExtractors
                                        .getOrDefault(statisticType,
                                                getUnsupportedStatisticsTypeThrower(
                                                        statisticType))
                                        .apply(vch.getField(),
                                                columnStatsMapEntry.getValue());
                                optionalDouble.ifPresent(
                                        doubleValue -> perColumnStatsBuilder.accept(
                                                statisticType, doubleValue));
                            });
                    ColumnStatistics colStatistics = perColumnStatsBuilder.build();
                    if (partitionColumns.contains(
                            fieldName) && rowCount > 0 && !colStatistics
                            .getDataSize()
                            .isUnknown()) {
                        // For partition columns, adjust data size to be sum of distinct values sizes across partitions
                        double partitionColumnDataSize = colStatistics
                                .getDataSize()
                                .getValue() * colStatistics
                                .getDistinctValuesCount()
                                .getValue() / rowCount;
                        LOG.debug(
                                "TRANSFORM dataSize of column %s from %s -> %s",
                                fieldName,
                                colStatistics.getDataSize().getValue(),
                                partitionColumnDataSize);
                        colStatistics = new ColumnStatistics(
                                colStatistics.getNullsFraction(),
                                colStatistics.getDistinctValuesCount(),
                                Estimate.of(partitionColumnDataSize),
                                colStatistics.getRange());
                    }
                    return colStatistics;
                }));
    }

    private List<ColumnStatisticMetadata> getColumnStatisticMetadata(
            VastColumnHandle vastColumnHandle)
    {
        String columnName = vastColumnHandle.getField().getName();
        return getSupportedColumnStatistics(
                TypeUtils.convertArrowFieldToTrinoType(
                        vastColumnHandle.getField()))
                .stream()
                .map(type -> type.createColumnStatisticMetadata(columnName))
                .collect(toImmutableList());
    }
}
