/*
 *  Copyright (C) Vast Data Ltd.
 */
package spark.sql.catalog.ndb;

import org.apache.iceberg.util.BucketUtil;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Optional;
import java.util.function.Function;

public class BucketFunction
        extends org.apache.iceberg.spark.functions.BucketFunction
{
    public static Optional<Predicate> apply(Predicate predicate,
            NamedReference newRef, int numBuckets)
    {
        if (predicate.children().length == 1) {
            return Optional.of(
                    new Predicate(predicate.name(), new Expression[] {newRef}));
        }
        if (!predicate.name().equals("=")) {
            return Optional.empty();
        }
        final Literal<?> literal = (Literal<?>) predicate.children()[1];
        DataType type = literal.dataType();
        Function<Literal<?>, ?> transform;
        if (type instanceof DateType || type instanceof ByteType || type instanceof ShortType || type instanceof IntegerType) {
            transform = l -> (BucketUtil.hash(
                    (Integer) l.value()) & Integer.MAX_VALUE) % numBuckets;
        }
        else if (type instanceof LongType || type instanceof TimestampType || type instanceof TimestampNTZType) {
            transform = l -> (BucketUtil.hash(
                    (Long) l.value()) & Integer.MAX_VALUE) % numBuckets;
        }
        else if (type instanceof DecimalType) {
            transform = l -> (BucketUtil.hash(
                    ((Decimal) l.value()).toJavaBigDecimal()) & Integer.MAX_VALUE) % numBuckets;
        }
        else if (type instanceof StringType) {
            transform = l -> (BucketUtil.hash(
                    ((UTF8String) l.value()).getBytes()) & Integer.MAX_VALUE) % numBuckets;
        }
        else if (type instanceof BinaryType) {
            transform = l -> (BucketUtil.hash(
                    (byte[]) l.value()) & Integer.MAX_VALUE) % numBuckets;
        }
        else {
            throw new UnsupportedOperationException(
                    "Expected column to be date, tinyint, smallint, int, bigint, decimal, timestamp, string, or binary");
        }
        Expression[] tl = new Expression[] {newRef, Expressions.literal(
                transform.apply(literal))};
        return Optional.of(new Predicate(predicate.name(), tl));
    }

    @Override
    public String description()
    {
        return name() + "(numBuckets, col) - Call Vast's bucket transform\n" + "  numBuckets :: number of buckets to divide the rows into, e.g. bucket(100, 34) -> 79 (must be an int)\n" + "  col :: column to bucket (must be a date, integer, long, timestamp, decimal, string, or binary)";
    }
}
