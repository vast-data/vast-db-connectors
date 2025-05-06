/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.base.CharMatcher;
import com.vastdata.client.VastExpressionSerializer;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.arrow.computeir.flatbuf.BinaryLiteral;
import org.apache.arrow.computeir.flatbuf.BooleanLiteral;
import org.apache.arrow.computeir.flatbuf.DateLiteral;
import org.apache.arrow.computeir.flatbuf.DecimalLiteral;
import org.apache.arrow.computeir.flatbuf.Deref;
import org.apache.arrow.computeir.flatbuf.Expression;
import org.apache.arrow.computeir.flatbuf.ExpressionImpl;
import org.apache.arrow.computeir.flatbuf.FieldIndex;
import org.apache.arrow.computeir.flatbuf.FieldRef;
import org.apache.arrow.computeir.flatbuf.FixedSizeBinaryLiteral;
import org.apache.arrow.computeir.flatbuf.Float32Literal;
import org.apache.arrow.computeir.flatbuf.Float64Literal;
import org.apache.arrow.computeir.flatbuf.Int16Literal;
import org.apache.arrow.computeir.flatbuf.Int32Literal;
import org.apache.arrow.computeir.flatbuf.Int64Literal;
import org.apache.arrow.computeir.flatbuf.Int8Literal;
import org.apache.arrow.computeir.flatbuf.Literal;
import org.apache.arrow.computeir.flatbuf.LiteralImpl;
import org.apache.arrow.computeir.flatbuf.StringLiteral;
import org.apache.arrow.computeir.flatbuf.TimeLiteral;
import org.apache.arrow.computeir.flatbuf.TimestampLiteral;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

abstract public class TrinoExpressionSerializer
        extends VastExpressionSerializer
{
    private static final Logger LOG = Logger.get(TrinoExpressionSerializer.class);

    protected int buildLiteral(Type type, Object value)
    {
        if (LOG.isDebugEnabled()) {
            LOG.debug("converting %s %s (%s) to Arrow literal", type, value, value.getClass());
        }

        Field field = TypeUtils.convertTrinoTypeToArrowField(type, null /*name*/, true /*nullable*/);
        ArrowType arrowType = field.getType();
        int fieldOffset = field.getField(builder);

        if (type.equals(BIGINT)) {
            return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                    builder,
                    LiteralImpl.Int64Literal,
                    Int64Literal.createInt64Literal(builder, (Long) value),
                    fieldOffset));
        }
        if (type.equals(INTEGER)) {
            return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                    builder,
                    LiteralImpl.Int32Literal,
                    Int32Literal.createInt32Literal(builder, ((Long) value).intValue()),
                    fieldOffset));
        }
        if (type.equals(SMALLINT)) {
            return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                    builder,
                    LiteralImpl.Int16Literal,
                    Int16Literal.createInt16Literal(builder, ((Long) value).shortValue()),
                    fieldOffset));
        }
        if (type.equals(TINYINT)) {
            return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                    builder,
                    LiteralImpl.Int8Literal,
                    Int8Literal.createInt8Literal(builder, ((Long) value).byteValue()),
                    fieldOffset));
        }
        if (type.equals(REAL)) {
            return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                    builder,
                    LiteralImpl.Float32Literal,
                    Float32Literal.createFloat32Literal(builder, intBitsToFloat(toIntExact((long) value))),
                    fieldOffset));
        }
        if (type.equals(DOUBLE)) {
            return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                    builder,
                    LiteralImpl.Float64Literal,
                    Float64Literal.createFloat64Literal(builder, (double) value),
                    fieldOffset));
        }
        if (type.equals(VARCHAR)) {
            return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                    builder,
                    LiteralImpl.StringLiteral,
                    StringLiteral.createStringLiteral(builder, builder.createString(((Slice) value).toStringUtf8())),
                    fieldOffset));
        }
        if (type instanceof CharType) {
            int length = ((CharType) type).getLength();
            String origPredicateStringValue = ((Slice) value).toStringUtf8();
            verify(CharMatcher.ascii().matchesAllOf(origPredicateStringValue), "CHAR type predicate pushdown is supported only for ASCII charset");
            // Works for ASCII translations only.
            // Trino does not pad values automatically, except when casting a char - Trino right-pads CHAR values: https://trino.io/docs/current/language/types.html#char
            // VAST requires padded values.
            String paddedPredicateStringValue = TypeUtils.rightPadSpaces(origPredicateStringValue, length);
            return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                    builder,
                    LiteralImpl.FixedSizeBinaryLiteral,
                    FixedSizeBinaryLiteral.createFixedSizeBinaryLiteral(builder, builder.createString(paddedPredicateStringValue)),
                    fieldOffset));
        }
        if (type.equals(DATE)) {
            return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                    builder,
                    LiteralImpl.DateLiteral,
                    DateLiteral.createDateLiteral(builder, (long) value),
                    fieldOffset));
        }
        if (type instanceof TimestampType) {
            TimeUnit unit = ((ArrowType.Timestamp) arrowType).getUnit();
            long result;
            if (unit == TimeUnit.NANOSECOND) {
                LongTimestamp ts = (LongTimestamp) value;
                result = TypeUtils.convertTwoValuesNanoToLong(ts.getEpochMicros(), ts.getPicosOfMicro()); // in nanos
            }
            else {
                // ShortTimestampType is represented in micros since Epoch (in Trino)
                long microsInUnit = TypeUtils.timeUnitToPicos(unit) / 1_000_000;
                long valueMicros = (long) value;
                if (valueMicros % microsInUnit != 0) {
                    throw new IllegalArgumentException(format("%s value %d be a multiple of %d", arrowType, valueMicros, microsInUnit));
                }
                result = valueMicros / microsInUnit; // rescale to use the correct TimeUnit (for Arrow)
            }
            return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                    builder,
                    LiteralImpl.TimestampLiteral,
                    TimestampLiteral.createTimestampLiteral(builder, result),
                    fieldOffset));
        }
        if (type instanceof TimestampWithTimeZoneType) {
            TimeUnit unit = ((ArrowType.Timestamp) arrowType).getUnit();
            long result;
            if (unit == TimeUnit.NANOSECOND || unit == TimeUnit.MICROSECOND) {
                LongTimestampWithTimeZone ts = (LongTimestampWithTimeZone) value;
                result = TypeUtils.convertTwoValuesNanoToLongMilli(ts.getEpochMillis(), ts.getPicosOfMilli()); // in nanos
            }
            else {
                // ShortTimestampWithTimeZoneType is represented in millis since Epoch (in Trino)
                long millisInUnit = TypeUtils.timeUnitToPicos(unit) / 1_000_000_000;
                long valueMillis = unpackMillisUtc((long) value);
                if (valueMillis % millisInUnit != 0) {
                    throw new IllegalArgumentException(format("%s value %d be a multiple of %d", arrowType, valueMillis, millisInUnit));
                }
                result = valueMillis / millisInUnit; // rescale to use the correct TimeUnit (for Arrow)
            }
            return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                    builder,
                    LiteralImpl.TimestampLiteral,
                    TimestampLiteral.createTimestampLiteral(builder, result),
                    fieldOffset));
        }
        if (type instanceof TimeType) {
            ArrowType.Time arrowTimeType = (ArrowType.Time) arrowType;
            long picosInUnit = TypeUtils.timeUnitToPicos(arrowTimeType.getUnit());
            long longValueInPicos = (long) value;
            if (longValueInPicos % picosInUnit != 0) {
                throw new IllegalArgumentException(format("%s value %d be a multiple of %d", arrowType, longValueInPicos, picosInUnit));
            }
            return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                    builder,
                    LiteralImpl.TimeLiteral,
                    TimeLiteral.createTimeLiteral(builder, longValueInPicos / picosInUnit),
                    fieldOffset));
        }
        if (type.equals(BOOLEAN)) {
            return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                    builder,
                    LiteralImpl.BooleanLiteral,
                    BooleanLiteral.createBooleanLiteral(builder, (boolean) value),
                    fieldOffset));
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            Int128 int128Value;
            if (decimalType.isShort()) {
                int128Value = Int128.valueOf((Long) value);
            }
            else {
                int128Value = (Int128) value;
            }
            byte[] bytes = new byte[Int128.SIZE];
            ByteBuffer
                    .wrap(bytes)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putLong(int128Value.getLow())
                    .putLong(int128Value.getHigh());
            int valueOffset = DecimalLiteral.createValueVector(builder, bytes);
            return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                    builder,
                    LiteralImpl.DecimalLiteral,
                    DecimalLiteral.createDecimalLiteral(builder, valueOffset),
                    fieldOffset));
        }
        if (type.equals(VARBINARY)) {
            Slice slice = (Slice) value;
            return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                    builder,
                    LiteralImpl.BinaryLiteral,
                    BinaryLiteral.createBinaryLiteral(builder, builder.createByteVector(slice.getBytes())),
                    fieldOffset));
        }
        if (type.equals(UUID)) {
            Slice slice = (Slice) value;
            return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                    builder,
                    LiteralImpl.FixedSizeBinaryLiteral,
                    FixedSizeBinaryLiteral.createFixedSizeBinaryLiteral(builder, builder.createByteVector(slice.getBytes())),
                    fieldOffset));
        }
        throw new UnsupportedOperationException(format("unsupported predicate pushdown for type=%s, value=%s", type, value));
    }
}
