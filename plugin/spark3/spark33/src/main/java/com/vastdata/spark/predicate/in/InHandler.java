/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.predicate.in;

import com.google.common.collect.ImmutableMap;
import com.vastdata.client.error.VastUserException;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.Map;

import static com.vastdata.client.VerifyParam.verify;
import static java.lang.String.format;

public final class InHandler
{
    private static final InValuesProcessor<Integer> exceptionThrower = new InValuesProcessor<Integer>() {
        @Override
        public int compare(Integer x, Integer y)
        {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public boolean isFullRange(Integer min, Integer max, int nullCount, Expression[] values)
        {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public ProcessedInValues<LiteralValue<?>> getProcessedInValues(Expression[] values, DataType dataType)
        {
            return null;
        }
    };

    private static class IntegerValuesProcessor
            implements InValuesProcessor<Integer>
    {
        @Override
        public int compare(Integer val1, Integer val2)
        {
            if (val1 == null || val2 == null) {
                throw new IllegalArgumentException(format("Values must not be null: %s, %s", val1, val2));
            }
            else {
                return Integer.compare(val1, val2);
            }
        }

        @Override
        public boolean isFullRange(Integer min, Integer max, int nullCount, Expression[] values)
        {
            if (max == null || min == null) {
                throw new IllegalArgumentException(format("Min or max values must not be null: %s, %s", max, min));
            }
            else {
                int range = max - min + 1;
                int nonNullValuesCount = values.length -1 - nullCount;
                return nonNullValuesCount == range;
            }
        }
    }

    private static class LongValuesProcessor
            implements InValuesProcessor<Long>
    {
        @Override
        public int compare(Long val1, Long val2)
        {
            if (val1 == null || val2 == null) {
                throw new IllegalArgumentException(format("Values must not be null: %s, %s", val1, val2));
            }
            else {
                return Long.compare(val1, val2);
            }
        }

        @Override
        public boolean isFullRange(Long min, Long max, int nullCount, Expression[] values)
        {
            if (max == null || min == null) {
                throw new IllegalArgumentException(format("Min or max values must not be null: %s, %s", max, min));
            }
            else {
                long range = max - min;
                if (range > Integer.MAX_VALUE) {
                    return false;
                }
                else {
                    int nonNullValuesCount = values.length - 1 - nullCount;
                    return nonNullValuesCount - (int)range == 1;
                }
            }
        }
    }

    private static final Map<DataType, InValuesProcessor<?>> typeExtractors =
            ImmutableMap.of(
                    DataTypes.DateType, new IntegerValuesProcessor(),
                    DataTypes.IntegerType, new IntegerValuesProcessor(),
                    DataTypes.LongType, new LongValuesProcessor()
            );


    private InHandler() {}

    public static Predicate[] extract(StructField field, Expression[] children, ResultFunction resultFunction)
            throws VastUserException
    {
        NamedReference ref = children[0].references()[0];
        String refFieldName = ref.fieldNames()[0];
        String fieldName = field.name();
        verify(refFieldName.equals(fieldName), format("Field names don't match: ref: %s, field: %s", refFieldName, fieldName));

        DataType dataType = field.dataType();
        return typeExtractors.getOrDefault(dataType, exceptionThrower).processValues(children, dataType, resultFunction);
    }
}
