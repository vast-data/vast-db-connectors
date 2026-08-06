/*
 *  Copyright (C) Vast Data Ltd.
 */
package spark.sql.catalog.ndb;

import org.apache.iceberg.util.BucketUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;

public class BoundBucketFunction
        implements UnboundFunction
{
    final int numBuckets;

    public BoundBucketFunction(int n)
    {
        numBuckets = n;
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
        if (type instanceof DateType) {
            return new BucketInt(numBuckets, type);
        }
        else if (type instanceof ByteType || type instanceof ShortType || type instanceof IntegerType) {
            return new BucketInt(numBuckets, DataTypes.IntegerType);
        }
        else if (type instanceof LongType) {
            return new BucketLong(numBuckets, type);
        }
        else if (type instanceof TimestampType) {
            return new BucketLong(numBuckets, type);
        }
        else if (type instanceof TimestampNTZType) {
            return new BucketLong(numBuckets, type);
        }
        else if (type instanceof DecimalType) {
            return new BucketDecimal(numBuckets, type);
        }
        else if (type instanceof StringType) {
            return new BucketString(numBuckets);
        }
        else if (type instanceof BinaryType) {
            return new BucketBinary(numBuckets);
        }
        else {
            throw new UnsupportedOperationException(
                    "Expected column to be date, tinyint, smallint, int, bigint, decimal, timestamp, string, or binary");
        }
    }

    @Override
    public String description()
    {
        return name() + "(col) - Call Vast's bucket transform\n" + "  col :: column to bucket (must be a date, integer, long, timestamp, decimal, string, or binary)";
    }

    @Override
    public String name()
    {
        return "bucket";
    }

    public abstract static class BucketBase
            implements ScalarFunction<Integer>
    {
        final int numBuckets;

        public BucketBase(int n)
        {
            numBuckets = n;
        }

        public int apply(int hashedValue)
        {
            return (hashedValue & Integer.MAX_VALUE) % numBuckets;
        }

        @Override
        public String name()
        {
            return "bucket";
        }

        @Override
        public DataType resultType()
        {
            return DataTypes.IntegerType;
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

    public static class BucketInt
            extends BucketBase
    {
        private final DataType sqlType;

        public BucketInt(int n, DataType sqlType)
        {
            super(n);
            this.sqlType = sqlType;
        }

        @Override
        public DataType[] inputTypes()
        {
            return new DataType[] {sqlType};
        }

        @Override
        public String canonicalName()
        {
            return String.format("ndb.bucket(%s)", sqlType.catalogString());
        }

        @Override
        public Integer produceResult(InternalRow input)
        {
            // return null for null input to match what Spark does in the code-generated versions.
            if (input.isNullAt(0)) {
                return null;
            }
            else {
                return apply(BucketUtil.hash(input.getInt(0)));
            }
        }
    }

    // Used for both BigInt and Timestamp
    public static class BucketLong
            extends BucketBase
    {
        private final DataType sqlType;

        public BucketLong(int n, DataType sqlType)
        {
            super(n);
            this.sqlType = sqlType;
        }

        @Override
        public DataType[] inputTypes()
        {
            return new DataType[] {sqlType};
        }

        @Override
        public String canonicalName()
        {
            return String.format("ndb.bucket(%s)", sqlType.catalogString());
        }

        @Override
        public Integer produceResult(InternalRow input)
        {
            if (input.isNullAt(0)) {
                return null;
            }
            else {
                return apply(BucketUtil.hash(input.getLong(0)));
            }
        }
    }

    public static class BucketString
            extends BucketBase
    {
        public BucketString(int n)
        {
            super(n);
        }

        @Override
        public DataType[] inputTypes()
        {
            return new DataType[] {DataTypes.StringType};
        }

        @Override
        public String canonicalName()
        {
            return "ndb.bucket(string)";
        }

        @Override
        public Integer produceResult(InternalRow input)
        {
            if (input.isNullAt(0)) {
                return null;
            }
            else {
                return apply(
                        BucketUtil.hash(input.getUTF8String(0).getBytes()));
            }
        }
    }

    public static class BucketBinary
            extends BucketBase
    {
        public BucketBinary(int n)
        {
            super(n);
        }

        @Override
        public DataType[] inputTypes()
        {
            return new DataType[] {DataTypes.BinaryType};
        }

        @Override
        public Integer produceResult(InternalRow input)
        {
            if (input.isNullAt(0)) {
                return null;
            }
            else {
                return apply(BucketUtil.hash(input.getBinary(0)));
            }
        }

        @Override
        public String canonicalName()
        {
            return "ndb.bucket(binary)";
        }
    }

    public static class BucketDecimal
            extends BucketBase
    {
        private final DataType sqlType;
        private final int precision;
        private final int scale;

        public BucketDecimal(int n, DataType sqlType)
        {
            super(n);
            this.sqlType = sqlType;
            this.precision = ((DecimalType) sqlType).precision();
            this.scale = ((DecimalType) sqlType).scale();
        }

        @Override
        public DataType[] inputTypes()
        {
            return new DataType[] {sqlType};
        }

        @Override
        public Integer produceResult(InternalRow input)
        {
            if (input.isNullAt(0)) {
                return null;
            }
            else {
                Decimal value = input.getDecimal(0, precision, scale);
                return apply(BucketUtil.hash(value.toJavaBigDecimal()));
            }
        }

        @Override
        public String canonicalName()
        {
            return "ndb.bucket(decimal)";
        }
    }
}
