/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.partition;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Murmur3Hash32;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import static com.vastdata.client.schema.ArrowSchemaUtils.VASTDB_EXTERNAL_ROW_ID_COLUMN_NAME;
import static com.vastdata.trino.TypeUtils.getTimestampWithTimeZone;
import static com.vastdata.trino.TypeUtils.timestampWithTimeZoneToMicros;
import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_HOUR;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.floorDiv;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;

public final class PartitionTransforms
{
    private static final DateTimeField YEAR_FIELD = ISOChronology
            .getInstanceUTC()
            .year();
    private static final DateTimeField MONTH_FIELD = ISOChronology
            .getInstanceUTC()
            .monthOfYear();

    private PartitionTransforms()
    {
    }

    public static ColumnTransform getColumnTransform(VastPartitionFunction field)
    {
        Type type = field.type();
        if (field
                .columnName()
                .equals(VASTDB_EXTERNAL_ROW_ID_COLUMN_NAME) && field.type() instanceof DecimalType) {
            return decimalRowIdTransform((DecimalType) type);
        }
        return switch (field.transform()) {
            case IDENTITY -> identity(type);
            case YEAR ->
                    dispatchTemporal(type, PartitionTransforms::yearsFromDate,
                            PartitionTransforms::yearsFromTimestamp,
                            PartitionTransforms::yearsFromTimestampWithTimeZone);
            case MONTH ->
                    dispatchTemporal(type, PartitionTransforms::monthsFromDate,
                            PartitionTransforms::monthsFromTimestamp,
                            PartitionTransforms::monthsFromTimestampWithTimeZone);
            case DAY ->
                    dispatchTemporal(type, PartitionTransforms::daysFromDate,
                            PartitionTransforms::daysFromTimestamp,
                            PartitionTransforms::daysFromTimestampWithTimeZone);
            case HOUR -> dispatchTemporal(type, null,
                    PartitionTransforms::hoursFromTimestamp,
                    PartitionTransforms::hoursFromTimestampWithTimeZone);
            case BUCKET -> bucket(type, field.size().orElseThrow());
            case TRUNCATE -> {
                int width = field.size().orElseThrow();
                if (type.equals(INTEGER)) {
                    yield truncateInteger(width);
                }
                if (type.equals(BIGINT)) {
                    yield truncateBigint(width);
                }
                if (type instanceof DecimalType decimalType) {
                    if (decimalType.isShort()) {
                        yield truncateShortDecimal(type, width, decimalType);
                    }
                    yield truncateLongDecimal(type, width, decimalType);
                }
                if (type instanceof VarcharType) {
                    yield truncateVarchar(width);
                }
                if (type.equals(VARBINARY)) {
                    yield truncateVarbinary(width);
                }
                throw new UnsupportedOperationException(
                        "Unsupported type for 'truncate': " + field);
            }
        };
    }

    private static ColumnTransform dispatchTemporal(Type type,
                                                    Supplier<ColumnTransform> fromDate,
                                                    Function<TimestampType, ColumnTransform> fromTimestamp,
                                                    Function<TimestampWithTimeZoneType, ColumnTransform> fromTimestampTz)
    {
        if (type.equals(DATE) && fromDate != null) {
            return fromDate.get();
        }
        if (type instanceof TimestampType t) {
            return fromTimestamp.apply(t);
        }
        if (type instanceof TimestampWithTimeZoneType t) {
            return fromTimestampTz.apply(t);
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    private static ColumnTransform identity(Type type)
    {
        return new ColumnTransform(type, false, true, false,
                Function.identity(), ValueTransform.identity(type));
    }

    @VisibleForTesting
    static ColumnTransform bucket(Type type, int count)
    {
        Hasher hasher = getBucketingHash(type);
        return new ColumnTransform(INTEGER, false, false, false,
                block -> bucketBlock(block, count, hasher), (block, position) ->
        {
            if (block.isNull(position)) {
                return null;
            }
            int hash = hasher.hash(block, position);
            int bucket = (hash & Integer.MAX_VALUE) % count;
            return (long) bucket;
        });
    }

    private static Hasher getBucketingHash(Type type)
    {
        if (type.equals(INTEGER)) {
            return PartitionTransforms::hashInteger;
        }
        if (type.equals(BIGINT)) {
            return PartitionTransforms::hashBigint;
        }
        if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                return hashShortDecimal(decimalType);
            }
            return hashLongDecimal(decimalType);
        }
        if (type.equals(DATE)) {
            return PartitionTransforms::hashDate;
        }
        if (type.equals(TIME_MICROS)) {
            return PartitionTransforms::hashTime;
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            return PartitionTransforms::hashTimestamp;
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            return PartitionTransforms.hashTimestampWithTimeZone(
                    timestampWithTimeZoneType);
        }
        if (type instanceof VarcharType) {
            return PartitionTransforms::hashVarchar;
        }
        if (type.equals(VARBINARY)) {
            return PartitionTransforms::hashVarbinary;
        }
        if (type.equals(UUID)) {
            return PartitionTransforms::hashUuid;
        }
        throw new UnsupportedOperationException(
                "Unsupported type for 'bucket': " + type);
    }

    private static ColumnTransform yearsFromDate()
    {
        LongUnaryOperator transform = value -> epochYear(DAYS.toMillis(value));
        return new ColumnTransform(INTEGER, false, true, true,
                block -> transformBlock(DATE, INTEGER, block, transform),
                ValueTransform.from(DATE, transform));
    }

    private static ColumnTransform monthsFromDate()
    {
        LongUnaryOperator transform = value -> epochMonth(DAYS.toMillis(value));
        return new ColumnTransform(INTEGER, false, true, true,
                block -> transformBlock(DATE, INTEGER, block, transform),
                ValueTransform.from(DATE, transform));
    }

    private static ColumnTransform decimalRowIdTransform(DecimalType type)
    {
        LongUnaryOperator transform = LongUnaryOperator.identity();
        return new ColumnTransform(INTEGER, false, true, true,
                block -> transformRowIdBlock(type, type, block, transform),
                ValueTransform.fromRowId(transform));
    }

    private static ColumnTransform daysFromDate()
    {
        LongUnaryOperator transform = LongUnaryOperator.identity();
        return new ColumnTransform(INTEGER, false, true, true,
                block -> transformBlock(DATE, INTEGER, block, transform),
                ValueTransform.from(DATE, transform));
    }

    private static ColumnTransform fromTimestamp(TimestampType sourceType,
                                                 LongUnaryOperator transform)
    {
        return new ColumnTransform(INTEGER, false, true, true, block ->
        {
            BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(
                    block.getPositionCount());
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    builder.appendNull();
                    continue;
                }
                long epochMicros;
                if (sourceType.isShort()) {
                    epochMicros = sourceType.getLong(block, position);
                }
                else {
                    epochMicros = ((LongTimestamp) sourceType.getObject(block,
                            position)).getEpochMicros();
                }
                long result = transform.applyAsLong(epochMicros);
                INTEGER.writeLong(builder, result);
            }
            return builder.build();
        }, (block, position) ->
        {
            if (block.isNull(position)) {
                return null;
            }
            long epochMicros;
            if (sourceType.isShort()) {
                epochMicros = sourceType.getLong(block, position);
            }
            else {
                epochMicros = ((LongTimestamp) sourceType.getObject(block,
                        position)).getEpochMicros();
            }
            return transform.applyAsLong(epochMicros);
        });
    }

    private static ColumnTransform yearsFromTimestamp(TimestampType sourceType)
    {
        LongUnaryOperator transform = epochMicros -> epochYear(
                floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND));
        return fromTimestamp(sourceType, transform);
    }

    private static ColumnTransform monthsFromTimestamp(TimestampType sourceType)
    {
        LongUnaryOperator transform = epochMicros -> epochMonth(
                floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND));
        return fromTimestamp(sourceType, transform);
    }

    private static ColumnTransform daysFromTimestamp(TimestampType sourceType)
    {
        LongUnaryOperator transform = epochMicros -> epochDay(
                floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND));
        return fromTimestamp(sourceType, transform);
    }

    private static ColumnTransform hoursFromTimestamp(TimestampType sourceType)
    {
        LongUnaryOperator transform = epochMicros -> epochHour(
                floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND));
        return fromTimestamp(sourceType, transform);
    }

    private static ColumnTransform yearsFromTimestampWithTimeZone(
            TimestampWithTimeZoneType timestampWithTimeZoneType)
    {
        ToLongFunction<LongTimestampWithTimeZone> transform = value -> epochYear(
                value.getEpochMillis());
        return new ColumnTransform(INTEGER, false, true, true,
                block -> extractTimestampWithTimeZone(block, transform,
                        timestampWithTimeZoneType),
                ValueTransform.fromTimestampTzTransform(transform,
                        timestampWithTimeZoneType));
    }

    private static ColumnTransform monthsFromTimestampWithTimeZone(
            TimestampWithTimeZoneType timestampWithTimeZoneType)
    {
        ToLongFunction<LongTimestampWithTimeZone> transform = value -> epochMonth(
                value.getEpochMillis());
        return new ColumnTransform(INTEGER, false, true, true,
                block -> extractTimestampWithTimeZone(block, transform,
                        timestampWithTimeZoneType),
                ValueTransform.fromTimestampTzTransform(transform,
                        timestampWithTimeZoneType));
    }

    private static ColumnTransform daysFromTimestampWithTimeZone(
            TimestampWithTimeZoneType timestampWithTimeZoneType)
    {
        ToLongFunction<LongTimestampWithTimeZone> transform = value -> epochDay(
                value.getEpochMillis());
        return new ColumnTransform(INTEGER, false, true, true,
                block -> extractTimestampWithTimeZone(block, transform,
                        timestampWithTimeZoneType),
                ValueTransform.fromTimestampTzTransform(transform,
                        timestampWithTimeZoneType));
    }

    private static ColumnTransform hoursFromTimestampWithTimeZone(
            TimestampWithTimeZoneType timestampWithTimeZoneType)
    {
        ToLongFunction<LongTimestampWithTimeZone> transform = value -> epochHour(
                value.getEpochMillis());
        return new ColumnTransform(INTEGER, false, true, true,
                block -> extractTimestampWithTimeZone(block, transform,
                        timestampWithTimeZoneType),
                ValueTransform.fromTimestampTzTransform(transform,
                        timestampWithTimeZoneType));
    }

    private static Block extractTimestampWithTimeZone(Block block,
                                                      ToLongFunction<LongTimestampWithTimeZone> function,
                                                      TimestampWithTimeZoneType timestampWithTimeZoneType)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(
                block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            LongTimestampWithTimeZone value = getTimestampWithTimeZone(block,
                    position, timestampWithTimeZoneType);
            INTEGER.writeLong(builder, function.applyAsLong(value));
        }
        return builder.build();
    }

    private static int hashInteger(Block block, int position)
    {
        return bucketHash(INTEGER.getInt(block, position));
    }

    private static int hashBigint(Block block, int position)
    {
        return bucketHash(BIGINT.getLong(block, position));
    }

    private static Hasher hashShortDecimal(DecimalType decimal)
    {
        return (block, position) ->
        {
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(decimal, block, position);
            return bucketHash(
                    Slices.wrappedBuffer(value.unscaledValue().toByteArray()));
        };
    }

    private static Hasher hashLongDecimal(DecimalType decimal)
    {
        return (block, position) ->
        {
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(decimal, block, position);
            return bucketHash(
                    Slices.wrappedBuffer(value.unscaledValue().toByteArray()));
        };
    }

    private static int hashDate(Block block, int position)
    {
        return bucketHash(DATE.getInt(block, position));
    }

    private static int hashTime(Block block, int position)
    {
        long picos = TIME_MICROS.getLong(block, position);
        return bucketHash(picos / PICOSECONDS_PER_MICROSECOND);
    }

    private static int hashTimestamp(Block block, int position)
    {
        return bucketHash(TIMESTAMP_MICROS.getLong(block, position));
    }

    private static Hasher hashTimestampWithTimeZone(TimestampWithTimeZoneType timestampWithTimeZoneType)
    {
        class TimestampWithTimeZoneHasher
                implements Hasher
        {
            @Override
            public int hash(Block block, int position)
            {
                return bucketHash(timestampWithTimeZoneToMicros(
                        getTimestampWithTimeZone(block, position,
                                timestampWithTimeZoneType)));
            }
        }

        return new TimestampWithTimeZoneHasher();
    }

    private static int hashVarchar(Block block, int position)
    {
        return bucketHash(VARCHAR.getSlice(block, position));
    }

    private static int hashVarbinary(Block block, int position)
    {
        return bucketHash(VARBINARY.getSlice(block, position));
    }

    private static int hashUuid(Block block, int position)
    {
        return bucketHash(UUID.getSlice(block, position));
    }

    private static Block bucketBlock(Block block, int count, Hasher hasher)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(
                block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            int hash = hasher.hash(block, position);
            int bucket = (hash & Integer.MAX_VALUE) % count;
            INTEGER.writeLong(builder, bucket);
        }
        return builder.build();
    }

    private static int bucketHash(long value)
    {
        return Murmur3Hash32.hash(value);
    }

    private static int bucketHash(Slice value)
    {
        return Murmur3Hash32.hash(value);
    }

    private static ColumnTransform truncateInteger(int width)
    {
        return new ColumnTransform(INTEGER, false, true, false,
                block -> truncateInteger(block, width), (block, position) ->
        {
            if (block.isNull(position)) {
                return null;
            }
            return truncateInteger(block, position, width);
        });
    }

    private static Block truncateInteger(Block block, int width)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(
                block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            INTEGER.writeLong(builder, truncateInteger(block, position, width));
        }
        return builder.build();
    }

    private static long truncateInteger(Block block, int position, int width)
    {
        long value = INTEGER.getInt(block, position);
        return value - ((value % width) + width) % width;
    }

    private static ColumnTransform truncateBigint(int width)
    {
        return new ColumnTransform(BIGINT, false, true, false,
                block -> truncateBigint(block, width), (block, position) ->
        {
            if (block.isNull(position)) {
                return null;
            }
            return truncateBigint(block, position, width);
        });
    }

    private static Block truncateBigint(Block block, int width)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(
                block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            BIGINT.writeLong(builder, truncateBigint(block, position, width));
        }
        return builder.build();
    }

    private static long truncateBigint(Block block, int position, int width)
    {
        long value = BIGINT.getLong(block, position);
        return value - ((value % width) + width) % width;
    }

    private static ColumnTransform truncateShortDecimal(Type type,
                                                        int width,
                                                        DecimalType decimal)
    {
        BigInteger unscaledWidth = BigInteger.valueOf(width);
        return new ColumnTransform(type, false, true, false,
                block -> truncateShortDecimal(decimal, block, unscaledWidth),
                (block, position) ->
                {
                    if (block.isNull(position)) {
                        return null;
                    }
                    return truncateShortDecimal(decimal, block, position,
                            unscaledWidth);
                });
    }

    private static Block truncateShortDecimal(DecimalType type,
                                              Block block,
                                              BigInteger unscaledWidth)
    {
        BlockBuilder builder = type.createFixedSizeBlockBuilder(
                block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            type.writeLong(builder,
                    truncateShortDecimal(type, block, position, unscaledWidth));
        }
        return builder.build();
    }

    private static long truncateShortDecimal(DecimalType type,
                                             Block block,
                                             int position,
                                             BigInteger unscaledWidth)
    {
        // TODO: write optimized implementation
        BigDecimal value = readBigDecimal(type, block, position);
        BigDecimal truncated = truncateDecimal(value, unscaledWidth);
        return encodeShortScaledValue(truncated, type.getScale());
    }

    private static ColumnTransform truncateLongDecimal(Type type,
                                                       int width,
                                                       DecimalType decimal)
    {
        BigInteger unscaledWidth = BigInteger.valueOf(width);
        return new ColumnTransform(type, false, true, false,
                block -> truncateLongDecimal(decimal, block, unscaledWidth),
                (block, position) ->
                {
                    if (block.isNull(position)) {
                        return null;
                    }
                    return truncateLongDecimal(decimal, block, position,
                            unscaledWidth);
                });
    }

    private static Block truncateLongDecimal(DecimalType type,
                                             Block block,
                                             BigInteger unscaledWidth)
    {
        BlockBuilder builder = type.createFixedSizeBlockBuilder(
                block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            type.writeObject(builder,
                    truncateLongDecimal(type, block, position, unscaledWidth));
        }
        return builder.build();
    }

    private static Int128 truncateLongDecimal(DecimalType type,
                                              Block block,
                                              int position,
                                              BigInteger unscaledWidth)
    {
        // TODO: write optimized implementation
        BigDecimal value = readBigDecimal(type, block, position);
        BigDecimal truncated = truncateDecimal(value, unscaledWidth);
        return encodeScaledValue(truncated, type.getScale());
    }

    private static BigDecimal truncateDecimal(BigDecimal value,
                                              BigInteger unscaledWidth)
    {
        BigDecimal remainder = new BigDecimal(value
                .unscaledValue()
                .remainder(unscaledWidth)
                .add(unscaledWidth)
                .remainder(unscaledWidth), value.scale());
        return value.subtract(remainder);
    }

    private static ColumnTransform truncateVarchar(int width)
    {
        return new ColumnTransform(VARCHAR, false, true, false,
                block -> truncateVarchar(block, width), (block, position) ->
        {
            if (block.isNull(position)) {
                return null;
            }
            return truncateVarchar(VARCHAR.getSlice(block, position), width);
        });
    }

    private static Block truncateVarchar(Block block, int width)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null,
                block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            Slice value = VARCHAR.getSlice(block, position);
            VARCHAR.writeSlice(builder, truncateVarchar(value, width));
        }
        return builder.build();
    }

    private static Slice truncateVarchar(Slice value, int max)
    {
        if (value.length() <= max) {
            return value;
        }
        int end = offsetOfCodePoint(value, 0, max);
        if (end < 0) {
            return value;
        }
        return value.slice(0, end);
    }

    private static ColumnTransform truncateVarbinary(int width)
    {
        return new ColumnTransform(VARBINARY, false, true, false,
                block -> truncateVarbinary(block, width), (block, position) ->
        {
            if (block.isNull(position)) {
                return null;
            }
            return truncateVarbinary(VARBINARY.getSlice(block, position),
                    width);
        });
    }

    private static Block truncateVarbinary(Block block, int width)
    {
        BlockBuilder builder = VARBINARY.createBlockBuilder(null,
                block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            Slice value = VARBINARY.getSlice(block, position);
            VARBINARY.writeSlice(builder, truncateVarbinary(value, width));
        }
        return builder.build();
    }

    private static Slice truncateVarbinary(Slice value, int width)
    {
        if (value.length() <= width) {
            return value;
        }
        return value.slice(0, width);
    }

    private static Block transformBlock(Type sourceType,
                                        FixedWidthType resultType,
                                        Block block,
                                        LongUnaryOperator function)
    {
        BlockBuilder builder = resultType.createFixedSizeBlockBuilder(
                block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long value = sourceType.getLong(block, position);
            resultType.writeLong(builder, function.applyAsLong(value));
        }
        return builder.build();
    }

    private static Block transformRowIdBlock(Type sourceType,
                                             FixedWidthType resultType,
                                             Block block,
                                             LongUnaryOperator function)
    {
        BlockBuilder builder = resultType.createFixedSizeBlockBuilder(
                block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            long value = sourceType.getLong(block, position);
            resultType.writeLong(builder, function.applyAsLong(value));
        }
        return builder.build();
    }

    @VisibleForTesting
    static long epochYear(long epochMilli)
    {
        return YEAR_FIELD.get(epochMilli) - 1970L;
    }

    @VisibleForTesting
    static long epochMonth(long epochMilli)
    {
        long year = epochYear(epochMilli);
        int month = MONTH_FIELD.get(epochMilli) - 1;
        return (year * 12) + month;
    }

    @VisibleForTesting
    static long epochDay(long epochMilli)
    {
        return floorDiv(epochMilli, MILLISECONDS_PER_DAY);
    }

    @VisibleForTesting
    static long epochHour(long epochMilli)
    {
        return floorDiv(epochMilli, MILLISECONDS_PER_HOUR);
    }

    private interface Hasher
    {
        int hash(Block block, int position);
    }

    public interface ValueTransform
    {
        static ValueTransform identity(Type type)
        {
            return (block, position) -> readNativeValue(type, block, position);
        }

        static ValueTransform from(Type sourceType, LongUnaryOperator transform)
        {
            return (block, position) ->
            {
                if (block.isNull(position)) {
                    return null;
                }
                return transform.applyAsLong(
                        sourceType.getLong(block, position));
            };
        }

        static ValueTransform fromRowId(LongUnaryOperator transform)
        {
            return (block, position) ->
            {
                if (block.isNull(position)) {
                    return null;
                }
                Int128ArrayBlock int128BLock = (Int128ArrayBlock) block;
                return transform.applyAsLong(
                        int128BLock.getInt128High(position));
            };
        }

        static ValueTransform fromTimestampTzTransform(ToLongFunction<LongTimestampWithTimeZone> transform,
                                                       TimestampWithTimeZoneType timestampWithTimeZoneType)
        {
            return (block, position) ->
            {
                if (block.isNull(position)) {
                    return null;
                }
                return transform.applyAsLong(
                        getTimestampWithTimeZone(block, position,
                                timestampWithTimeZoneType));
            };
        }

        Object apply(Block block, int position);
    }

    /**
     * @param type Result type.
     */
    public record ColumnTransform(Type type,
            boolean preservesNonNull,
            boolean monotonic,
            boolean temporal,
            Function<Block, Block> blockTransform,
            ValueTransform valueTransform)
    {
        public ColumnTransform
        {
            requireNonNull(type, "type is null");
            requireNonNull(blockTransform, "transform is null");
            requireNonNull(valueTransform, "valueTransform is null");
        }
    }
}
