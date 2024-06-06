/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.predicate.in;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataType;


interface InValuesProcessor<T>
{
    int compare(T val1, T val2);

    // values = [col, null, n, ..., n+x] -> nonNullValuesCount=x+1, range=x, expected result: col>=n and x<=n+x
    // values = [col, null, n, n+2, ... n+y] | y > (min_max_compaction_min_values_threshold+1) -> nonNullValuesCount=y-1, range=y, expected result: col>=n OR col<=n+y
    // values = [col, null, n, n+3] -> nonNullValuesCount=2, range=3, expected result: col=n OR col=n+3
    boolean isFullRange(T max, T min, int nullCount, Expression[] values);

    default T getValue(Expression exp)
    {
        return ((LiteralValue<T>) exp).value();
    }

    default Predicate[] processValues(Expression[] values, DataType dataType, ResultFunction resultFunction)
    {
        ProcessedInValues res = getProcessedInValues(values, dataType);
        return resultFunction.apply(res, values);
    }

    default ProcessedInValues getProcessedInValues(Expression[] values, DataType dataType)
    {
        int nullCount = 0;
        T min = null;
        T max = null;
        for (int i = 1; i < values.length; i++) {
            T val = getValue(values[i]);
            if (val == null) {
                nullCount++;
            }
            else {
                if (min == null) {
                    min = val;
                    max = val;
                }
                else {
                    if (compare(val, min) < 0) {
                        min = val;
                    }
                    else if (compare(max, val) < 0) {
                        max = val;
                    }
                }
            }
        }
        boolean range = isFullRange(max, min, nullCount, values);
        LiteralValue<T> minLiteral = new LiteralValue<>(min, dataType);
        LiteralValue<T> maxLiteral = new LiteralValue<>(max, dataType);
        return new ProcessedInValues(minLiteral, maxLiteral, range, nullCount);
    }
}
