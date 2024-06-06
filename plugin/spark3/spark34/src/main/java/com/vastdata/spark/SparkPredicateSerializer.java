/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.base.CharMatcher;
import com.vastdata.client.VastExpressionSerializer;
import com.vastdata.client.schema.EnumeratedSchema;
import com.vastdata.spark.predicate.VastPredicate;
import org.apache.arrow.computeir.flatbuf.BinaryLiteral;
import org.apache.arrow.computeir.flatbuf.BooleanLiteral;
import org.apache.arrow.computeir.flatbuf.DateLiteral;
import org.apache.arrow.computeir.flatbuf.DecimalLiteral;
import org.apache.arrow.computeir.flatbuf.Expression;
import org.apache.arrow.computeir.flatbuf.ExpressionImpl;
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
import org.apache.arrow.computeir.flatbuf.TimestampLiteral;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.sql.catalog.ndb.TimestampPrecision;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static org.apache.arrow.vector.types.Types.MinorType.VARBINARY;
import static spark.sql.catalog.ndb.TypeUtil.TIMESTAMP_PRECISION;
import static spark.sql.catalog.ndb.TypeUtil.sparkFieldToArrowField;

public class SparkPredicateSerializer
        extends VastExpressionSerializer
{
    private static final Logger LOG = LoggerFactory.getLogger(SparkPredicateSerializer.class);

    private final String traceToken;
    // List contains AND-ed expressions, MultiMap contains OR-ed expressions
    private final List<List<VastPredicate>> andOredPredicates;
    private final EnumeratedSchema schema;
    private final Map<NamedReference, Integer> columnIndexMap;

    public SparkPredicateSerializer(String traceToken, List<List<VastPredicate>> predicates, EnumeratedSchema enumeratedSchema)
    {
        this.traceToken = traceToken;
        this.andOredPredicates = predicates;
        this.columnIndexMap = new HashMap<>();
        this.schema = enumeratedSchema;
    }

    private static int getColumnPosition(NamedReference column, EnumeratedSchema enumeratedSchema)
    {
        // TODO: only predicates over scalar columns are supported
        if (column.fieldNames().length != 1) {
            throw new UnsupportedOperationException("unsupported predicate pushdown for nested column: " +
                    Arrays.toString(column.fieldNames()));
        }
        String colName = column.fieldNames()[0];
        return enumeratedSchema.getBaseFieldIndexByName(colName);
    }

    @Override
    public int serialize()
    {
        long start = System.nanoTime();
        try {
            for (List<VastPredicate> oredList : this.andOredPredicates) {
                if (LOG.isDebugEnabled()) {
                    oredList.forEach(vp -> LOG.debug("{} Serializing predicate to Arrow flatbuffer: {}\n", traceToken, vp.getPredicate().toString()));
                }
                List<NamedReference> references = oredList.stream().map(VastPredicate::getReference).distinct().collect(Collectors.toList());
                if (references.size() != 1) {
                    throw new UnsupportedOperationException("Should have gotten a single column reference here, but got: " + references);
                }
                else {
                    NamedReference nrHead = references.get(0);
                    this.columnIndexMap.put(nrHead, buildColumn(getColumnPosition(nrHead, this.schema)));
                }
            }
            int[] offsets = new int[this.andOredPredicates.size()];
            int i = 0;
            for (List<VastPredicate> oredList : this.andOredPredicates) {
                offsets[i++] = buildOr(
                        serializeRanges(oredList)
                );
            }
            return buildAnd(offsets);
        }
        finally {
            long nanos = System.nanoTime() - start;
            LOG.info("{} Serialize took {} ns", traceToken, nanos);
        }
    }


    private int[] serializeRanges(List<VastPredicate> parsedOrPreds) {
        int[] offsets = new int[parsedOrPreds.size()];
        int i = 0;
        for (VastPredicate pred : parsedOrPreds) {
            LOG.debug("{} serializing ranges " + pred.getPredicate(), traceToken);
            offsets[i++] = serializeRange(pred);
        }
        return offsets;
    }

    private int serializeRange(VastPredicate pred) {
        int colIndex = this.columnIndexMap.get(pred.getReference());
        LOG.debug("{} serializing range {} for column position {}", traceToken, pred.getPredicate(), colIndex);
        switch (pred.getPredicate().name()) {
            case ">":
                return buildGreater(colIndex, serializeLiteral(pred.getField(), ((LiteralValue<?>) pred.getPredicate().children()[1])), false);
            case ">=":
                return buildGreater(colIndex, serializeLiteral(pred.getField(), ((LiteralValue<?>) pred.getPredicate().children()[1])), true);
            case "<":
                return buildLess(colIndex, serializeLiteral(pred.getField(), ((LiteralValue<?>) pred.getPredicate().children()[1])), false);
            case "<=":
                return buildLess(colIndex, serializeLiteral(pred.getField(), ((LiteralValue<?>) pred.getPredicate().children()[1])), true);
            case "=":
                return buildEqual(colIndex, serializeLiteral(pred.getField(), ((LiteralValue<?>) pred.getPredicate().children()[1])));
            case "IS_NULL":
                return buildIsNull(colIndex);
            case "IS_NOT_NULL":
                return buildIsValid(colIndex);
            case "AND":
            case "OR":
                VastPredicate[] childs = new VastPredicate[pred.getPredicate().children().length];
                for (int i = 0; i < pred.getPredicate().children().length; i++) {
                    childs[i] = new VastPredicate((Predicate) pred.getPredicate().children()[i], pred.getReference(), pred.getField());
                }
                if (Objects.equals(pred.getPredicate().name(), "AND"))
                    return buildAnd(serializeRanges(Arrays.asList(childs)));
                else
                    return buildOr(serializeRanges(Arrays.asList(childs)));
        }
        throw new UnsupportedOperationException("Unimplemented expression serializer " +  pred.getPredicate().name() + " pred:" + pred.toString());
    }

    private int serializeLiteral(StructField structField, LiteralValue<?> value) {
        DataType type = structField.dataType();
        if (LOG.isDebugEnabled()) {
            LOG.debug("{} converting {} {} ({}) to Arrow literal", traceToken, type, value, value.getClass());
        }
        Field field = sparkFieldToArrowField(null, type, true, structField.metadata());
        int fieldOffset = field.getField(builder);
        // Spark Data types specification:
        // https://spark.apache.org/docs/latest/sql-ref-datatypes.html
        if (type.equals(DataTypes.BooleanType)) {
            return buildLiteral(
                    LiteralImpl.BooleanLiteral,
                    BooleanLiteral.createBooleanLiteral(builder, ((LiteralValue<Boolean>) value).value()),
                    fieldOffset
            );
        }
        if (type.equals(DataTypes.ByteType)) {
            return buildLiteral(
                    LiteralImpl.Int8Literal,
                    Int8Literal.createInt8Literal(builder, ((LiteralValue<Byte>) value).value()),
                    fieldOffset
            );
        }
        if (type.equals(DataTypes.ShortType)) {
            return buildLiteral(
                    LiteralImpl.Int16Literal,
                    Int16Literal.createInt16Literal(builder, ((LiteralValue<Short>) value).value()),
                    fieldOffset
            );
        }
        if (type.equals(DataTypes.IntegerType)) {
            return buildLiteral(
                    LiteralImpl.Int32Literal,
                    Int32Literal.createInt32Literal(builder, ((LiteralValue<Integer>) value).value()),
                    fieldOffset
            );
        }
        if (type.equals(DataTypes.LongType)) {
            return buildLiteral(
                    LiteralImpl.Int64Literal,
                    Int64Literal.createInt64Literal(builder, ((LiteralValue<Long>) value).value()),
                    fieldOffset
            );
        }
        if (type.equals(DataTypes.FloatType)) {
            return buildLiteral(
                    LiteralImpl.Float32Literal,
                    Float32Literal.createFloat32Literal(builder, ((LiteralValue<Float>) value).value()),
                    fieldOffset
            );
        }
        if (type.equals(DataTypes.DoubleType)) {
            return buildLiteral(
                    LiteralImpl.Float64Literal,
                    Float64Literal.createFloat64Literal(builder, ((LiteralValue<Double>) value).value()),
                    fieldOffset
            );
        }
        if (type.equals(DataTypes.DateType)) {
            return buildLiteral(
                    LiteralImpl.DateLiteral,
                    DateLiteral.createDateLiteral(builder, ((LiteralValue<Integer>) value).value().longValue()),
                    fieldOffset
            );
        }
        if (type.equals(DataTypes.TimestampType)) {
            long longValue = ((LiteralValue<Long>) value).value().longValue();
            if (structField.metadata().contains(TIMESTAMP_PRECISION)) {
                long precisionID = structField.metadata().getLong(TIMESTAMP_PRECISION);
                TimestampPrecision timestampPrecision = TimestampPrecision.fromID((int) precisionID);
                if (timestampPrecision != null) {
                    long newLong;
                    switch (timestampPrecision) {
                        case SECONDS:
                            newLong = longValue / 1000000;
                            break;
                        case MILLISECONDS:
                            newLong = longValue / 1000;
                            break;
                        case MICROSECONDS:
                            newLong = longValue;
                            break;
                        case NANOSECONDS:
                            newLong = longValue * 1000;
                            break;
                        default:
                            throw new RuntimeException(format("Unexpected precision enum: %s", timestampPrecision));
                    }
                    return buildLiteral(
                            LiteralImpl.TimestampLiteral,
                            TimestampLiteral.createTimestampLiteral(builder, newLong),
                            fieldOffset
                    );
                }
                else {
                    throw new RuntimeException(format("Unexpected precision id: %s", precisionID));
                }
            }
            else {
                LOG.warn("Timestamp type {} has no metadata, defaulting to Microseconds precision", type);
                return buildLiteral(
                        LiteralImpl.TimestampLiteral,
                        TimestampLiteral.createTimestampLiteral(builder, longValue),
                        fieldOffset
                );
            }
        }
        if (type.equals(DataTypes.StringType)) {
            return buildLiteral(
                    LiteralImpl.StringLiteral,
                    StringLiteral.createStringLiteral(builder,
                            builder.createString(
                                    ((LiteralValue<UTF8String>) value).value().getByteBuffer())),
                    fieldOffset
            );
        }
        if (type instanceof CharType) {
            verify(CharMatcher.ascii().matchesAllOf(((LiteralValue<UTF8String>) value).value().toString()), "CHAR type predicate pushdown is supported only for ASCII charset");
            // Works for ASCII translations only.
            // Spark supports padding https://spark.apache.org/docs/latest/sql-ref-datatypes.html as VAST requires
            // TODO: 8/1/23 - Fixed len string is unsupported yet - remove this comment and test once implemented
            return buildLiteral(
                    LiteralImpl.FixedSizeBinaryLiteral,
                    FixedSizeBinaryLiteral.createFixedSizeBinaryLiteral(builder, builder.createString(((LiteralValue<UTF8String>) value).value().toString())),
                    fieldOffset
            );
        }
        if (type.equals(VARBINARY) || type.equals(DataTypes.BinaryType)) {
            return buildLiteral(LiteralImpl.BinaryLiteral,
                    BinaryLiteral.createBinaryLiteral(builder, builder.createByteVector((byte[]) value.value())),
                    fieldOffset
            );
        }
        if (type instanceof DecimalType) {
            byte[] bytes = decimalToByteArray(((LiteralValue<Decimal>) value).value());
            int valueOffset = DecimalLiteral.createValueVector(builder, bytes);
            return buildLiteral(LiteralImpl.DecimalLiteral,
                    DecimalLiteral.createDecimalLiteral(
                            builder,
                            valueOffset
                    ),
                    fieldOffset
            );
        }
//        TODO: handle DataTypes.CalendarIntervalType
        throw new UnsupportedOperationException("Unimplemented literal serializer " + type + " " + value.toString());
    }

    protected byte[] decimalToByteArray(Decimal decimal)
    {
        BigInteger bigInteger = decimal.toJavaBigDecimal().unscaledValue();
        long low = bigInteger.longValue(); // Adaptation to io.trino.spi.type.Int128 representation
        long high;
        try {
            high = bigInteger.shiftRight(64).longValueExact();
        }
        catch (ArithmeticException e) {
            throw new ArithmeticException("BigInteger out of Int128 range");
        }
        byte[] bytes = new byte[16];
        ByteBuffer
                .wrap(bytes)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(low)
                .putLong(high);

        return bytes;
    }

    private int buildLiteral(Byte literalImpl, int implOffset, int fieldOffset)
    {
        return Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                builder,
                literalImpl,
                implOffset,
                fieldOffset));
    }
}
