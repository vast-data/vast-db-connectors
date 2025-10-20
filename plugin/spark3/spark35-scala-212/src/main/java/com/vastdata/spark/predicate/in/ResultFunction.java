/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.predicate.in;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;

import java.util.function.BiFunction;

public abstract class ResultFunction
        implements BiFunction<ProcessedInValues<LiteralValue<?>>, Expression[], Predicate[]>
{
    private BiFunction<ProcessedInValues<LiteralValue<?>>, Expression[], Predicate[]> next = null;

    protected Predicate[] applyNext(ProcessedInValues<LiteralValue<?>> processedInValues, Expression[] values)
    {
        return this.next.apply(processedInValues, values);
    }

    protected void setNext(BiFunction<ProcessedInValues<LiteralValue<?>>, Expression[], Predicate[]> next)
    {
        this.next = next;
    }
}
