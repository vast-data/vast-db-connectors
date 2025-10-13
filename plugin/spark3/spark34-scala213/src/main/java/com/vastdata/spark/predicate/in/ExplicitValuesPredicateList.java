/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.predicate.in;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.filter.Predicate;

import java.util.stream.IntStream;

public class ExplicitValuesPredicateList extends ResultFunction
{
    @Override
    public Predicate[] apply(ProcessedInValues processedInValues, Expression[] values)
    {
        return IntStream.range(1, values.length)
                .mapToObj(i -> new Predicate("=", new Expression[] {values[0], values[i]}))
                .toArray(Predicate[]::new);
    }
}
