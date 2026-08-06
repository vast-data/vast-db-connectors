/*
 *  Copyright (C) Vast Data Ltd.
 */
package spark.sql.catalog.ndb;

import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.TruncateUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

public class BoundTruncateFunction
        implements UnboundFunction
{
    final int arg;

    public BoundTruncateFunction(int a)
    {
        arg = a;
    }

    public static Optional<Predicate> apply(Predicate predicate,
            NamedReference newRef, int width)
    {
        if (predicate.children().length == 1) {
            return Optional.of(
                    new Predicate(predicate.name(), new Expression[] {newRef}));
        }
        if (predicate.name().equals("AND")) {
            Optional<Predicate> lhs = apply((Predicate) predicate.children()[0],
                    newRef, width);
            Optional<Predicate> rhs = apply((Predicate) predicate.children()[1],
                    newRef, width);
            if (!lhs.isPresent()) {
                return rhs;
            }
            return rhs
                    .map(value -> Optional.of(new Predicate("AND",
                            new Expression[] {lhs.get(), value})))
                    .orElse(lhs);
        }
        final Literal<?> literal = (Literal<?>) predicate.children()[1];
        DataType type = literal.dataType();
        Function<Literal<?>, ?> transform;
        if (type instanceof ByteType) {
            transform = l -> TruncateUtil.truncateByte(width, (Byte) l.value());
        }
        else if (type instanceof ShortType) {
            transform = l -> TruncateUtil.truncateShort(width,
                    (Short) (l).value());
        }
        else if (type instanceof IntegerType) {
            transform = l -> TruncateUtil.truncateInt(width,
                    (Integer) (l).value());
        }
        else if (type instanceof LongType) {
            transform = l -> TruncateUtil.truncateLong(width,
                    (Long) (l).value());
        }
        else if (type instanceof DecimalType) {
            transform = l -> Decimal.apply(
                    TruncateUtil.truncateDecimal(BigInteger.valueOf(width),
                            ((Decimal) (l).value()).toJavaBigDecimal()));
        }
        else if (type instanceof StringType) {
            transform = l -> ((UTF8String) (l).value()).substring(0, width);
        }
        else if (type instanceof BinaryType) {
            transform = l -> ByteBuffers.toByteArray(
                    BinaryUtil.truncateBinaryUnsafe(
                            ByteBuffer.wrap((byte[]) (l).value()), width));
        }
        else {
            throw new UnsupportedOperationException(
                    "Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");
        }
        switch (predicate.name()) {
            case "<>":
            case "!=":
                return Optional.empty();
            case "=":
            case "<=":
            case ">=": {
                Expression[] tl = new Expression[] {newRef, Expressions.literal(
                        transform.apply(literal))};
                return Optional.of(new Predicate(predicate.name(), tl));
            }
            case ">":
            case "<": {
                // TODO: this might bring in an extra partition in corner cases.  See E.g., YearsFunction
                //       for a better approach.
                Expression[] tl = new Expression[] {newRef, Expressions.literal(
                        transform.apply(literal))};
                return Optional.of(new Predicate(predicate.name() + "=", tl));
            }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported predicate: " + predicate.name());
        }

    }

    @Override
    public BoundFunction bind(StructType inputType)
    {
        if (inputType.size() != 1) {
            throw new UnsupportedOperationException(
                    "Wrong number of inputs (expected a value)");
        }

        StructField valueField = inputType.fields()[0];

        DataType type = valueField.dataType();
        if (type instanceof ByteType) {
            return new TruncateTinyInt(arg);
        }
        else if (type instanceof ShortType) {
            return new TruncateSmallInt(arg);
        }
        else if (type instanceof IntegerType) {
            return new TruncateInt(arg);
        }
        else if (type instanceof LongType) {
            return new TruncateBigInt(arg);
        }
        else if (type instanceof DecimalType) {
            return new TruncateDecimal(arg, ((DecimalType) type).precision(),
                    ((DecimalType) type).scale());
        }
        else if (type instanceof StringType) {
            return new TruncateString(arg);
        }
        else if (type instanceof BinaryType) {
            return new TruncateBinary(arg);
        }
        else {
            throw new UnsupportedOperationException(
                    "Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");
        }
    }

    @Override
    public String description()
    {
        return name() + "(col) - Call Vast's truncate transform\n" + "  col :: column to truncate (must be an integer, decimal, string, or binary)";
    }

    @Override
    public String name()
    {
        return "truncate";
    }

    public abstract static class TruncateBase<T>
            implements ScalarFunction<T>
    {
        @Override
        public String name()
        {
            return "truncate";
        }

        @Override
        public int hashCode()
        {
            return canonicalName().hashCode();
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) {
                return true;
            }
            else if (!(other instanceof ScalarFunction)) {
                return false;
            }

            ScalarFunction<?> that = (ScalarFunction<?>) other;
            return canonicalName().equals(that.canonicalName());
        }
    }

    public static class TruncateTinyInt
            extends TruncateBase<Byte>
    {

        final int width;

        public TruncateTinyInt(int width)
        {
            this.width = width;
        }

        @Override
        public DataType[] inputTypes()
        {
            return new DataType[] {DataTypes.ByteType};
        }

        @Override
        public DataType resultType()
        {
            return DataTypes.ByteType;
        }

        @Override
        public String canonicalName()
        {
            return "ndb.truncate(tinyint)";
        }

        @Override
        public Byte produceResult(InternalRow input)
        {
            if (input.isNullAt(0)) {
                return null;
            }
            else {
                return TruncateUtil.truncateByte(width, input.getByte(0));
            }
        }
    }

    public static class TruncateSmallInt
            extends TruncateBase<Short>
    {
        final int width;

        public TruncateSmallInt(int width)
        {
            this.width = width;
        }

        @Override
        public DataType[] inputTypes()
        {
            return new DataType[] {DataTypes.ShortType};
        }

        @Override
        public DataType resultType()
        {
            return DataTypes.ShortType;
        }

        @Override
        public String canonicalName()
        {
            return "ndb.truncate(smallint)";
        }

        @Override
        public Short produceResult(InternalRow input)
        {
            if (input.isNullAt(0)) {
                return null;
            }
            else {
                return TruncateUtil.truncateShort(width, input.getShort(0));
            }
        }
    }

    public static class TruncateInt
            extends TruncateBase<Integer>
    {
        final int width;

        public TruncateInt(int width)
        {
            this.width = width;
        }

        @Override
        public DataType[] inputTypes()
        {
            return new DataType[] {DataTypes.IntegerType};
        }

        @Override
        public DataType resultType()
        {
            return DataTypes.IntegerType;
        }

        @Override
        public String canonicalName()
        {
            return "ndb.truncate(int)";
        }

        @Override
        public Integer produceResult(InternalRow input)
        {
            if (input.isNullAt(0)) {
                return null;
            }
            else {
                return TruncateUtil.truncateInt(width, input.getInt(0));
            }
        }
    }

    public static class TruncateBigInt
            extends TruncateBase<Long>
    {
        final int width;

        public TruncateBigInt(int width)
        {
            this.width = width;
        }

        @Override
        public DataType[] inputTypes()
        {
            return new DataType[] {DataTypes.LongType};
        }

        @Override
        public DataType resultType()
        {
            return DataTypes.LongType;
        }

        @Override
        public String canonicalName()
        {
            return "ndb.truncate(bigint)";
        }

        @Override
        public Long produceResult(InternalRow input)
        {
            if (input.isNullAt(0)) {
                return null;
            }
            else {
                return TruncateUtil.truncateLong(width, input.getLong(0));
            }
        }
    }

    public static class TruncateString
            extends TruncateBase<UTF8String>
    {
        final int width;

        public TruncateString(int width)
        {
            this.width = width;
        }

        @Override
        public DataType[] inputTypes()
        {
            return new DataType[] {DataTypes.StringType};
        }

        @Override
        public DataType resultType()
        {
            return DataTypes.StringType;
        }

        @Override
        public String canonicalName()
        {
            return "ndb.truncate(string)";
        }

        @Override
        public UTF8String produceResult(InternalRow input)
        {
            if (input.isNullAt(0) || input.getUTF8String(0) == null) {
                return null;
            }
            else {
                return input.getUTF8String(0).substring(0, width);
            }
        }
    }

    public static class TruncateBinary
            extends TruncateBase<byte[]>
    {
        final int width;

        public TruncateBinary(int width)
        {
            this.width = width;
        }

        @Override
        public DataType[] inputTypes()
        {
            return new DataType[] {DataTypes.BinaryType};
        }

        @Override
        public DataType resultType()
        {
            return DataTypes.BinaryType;
        }

        @Override
        public String canonicalName()
        {
            return "ndb.truncate(binary)";
        }

        @Override
        public byte[] produceResult(InternalRow input)
        {
            if (input.isNullAt(0) || input.getBinary(0) == null) {
                return null;
            }
            else {
                return ByteBuffers.toByteArray(BinaryUtil.truncateBinaryUnsafe(
                        ByteBuffer.wrap(input.getBinary(0)), width));
            }
        }
    }

    public static class TruncateDecimal
            extends TruncateBase<Decimal>
    {
        private final int precision;
        private final int scale;
        private final int width;

        public TruncateDecimal(int width, int precision, int scale)
        {
            this.precision = precision;
            this.scale = scale;
            this.width = width;
        }

        // magic method used in codegen
        public static Decimal invoke(int width, Decimal value)
        {
            if (value == null) {
                return null;
            }

            return Decimal.apply(
                    TruncateUtil.truncateDecimal(BigInteger.valueOf(width),
                            value.toJavaBigDecimal()));
        }

        @Override
        public DataType[] inputTypes()
        {
            return new DataType[] {DataTypes.createDecimalType(precision,
                    scale)};
        }

        @Override
        public DataType resultType()
        {
            return DataTypes.createDecimalType(precision, scale);
        }

        @Override
        public String canonicalName()
        {
            return String.format(Locale.ROOT, "ndb.truncate(decimal(%d,%d))",
                    precision, scale);
        }

        @Override
        public Decimal produceResult(InternalRow input)
        {
            if (input.isNullAt(0) || input.getDecimal(0, precision,
                    scale) == null) {
                return null;
            }
            else {
                Decimal value = input.getDecimal(0, precision, scale);
                return Decimal.apply(
                        TruncateUtil.truncateDecimal(BigInteger.valueOf(width),
                                value.toJavaBigDecimal()));
            }
        }
    }
}
