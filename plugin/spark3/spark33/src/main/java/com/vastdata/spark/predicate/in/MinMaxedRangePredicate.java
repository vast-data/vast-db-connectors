/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.predicate.in;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;
import java.util.function.Function;

public class MinMaxedRangePredicate extends ResultFunction
{
    private static final Logger LOG = LoggerFactory.getLogger(MinMaxedRangePredicate.class);
    public static final BiFunction<Predicate, Predicate, Predicate> MIN_MAX_PREDICATE = (min, max) -> new Predicate("AND", new Predicate[] {min, max});
    public static final Function<Expression, Predicate> IS_NULL_PREDICATE = col -> new Predicate("IS_NULL", new Expression[] {col});

    private final boolean fullRangeOnly;
    private final int threshold;


    private MinMaxedRangePredicate(boolean fullRangeOnly, int threshold) {
        super();
        this.fullRangeOnly = fullRangeOnly;
        this.threshold = threshold;
    }

    @Override
    public Predicate[] apply(ProcessedInValues<LiteralValue<?>> processedInValues, Expression[] values)
    {
        int nonNullValuesCount = values.length - processedInValues.getNullCount() - 1; // the first is the column NamedReference
        if (processedInValues.isFullRange()) {
            LOG.info("Optimizing full range: {}", processedInValues);
            return getPredicate(processedInValues, values, processedInValues.getNullCount() > 0);
        }
        if (!fullRangeOnly && nonNullValuesCount > threshold) {
            LOG.info("Optimizing partial range: {}", processedInValues);
            return getPredicate(processedInValues, values, processedInValues.getNullCount() > 0);
        }
        else {
            LOG.info("Skipping optimization for range: {}, values length: {}, threshold: {}", processedInValues, values.length - 1, threshold);
            return applyNext(processedInValues, values);
        }
    }

    @NotNull
    private static Predicate[] getPredicate(ProcessedInValues<LiteralValue<?>> processedInValues, Expression[] values, boolean hasNulls)
    {
        Predicate lteMax = new Predicate("<=", new Expression[]{values[0], processedInValues.getMax()});
        Predicate gteMin = new Predicate(">=", new Expression[]{values[0], processedInValues.getMin()});
        Predicate minMax = MIN_MAX_PREDICATE.apply(gteMin, lteMax);
        if (hasNulls) {
            return new Predicate[] {minMax, IS_NULL_PREDICATE.apply(values[0])};
        }
        else {
            return new Predicate[] {minMax};
        }
    }

    public static MinMaxedRangePredicate fullRangeOnly()
    {
        return new MinMaxedRangePredicate(true, Integer.MAX_VALUE);
    }

    public static MinMaxedRangePredicate simple(int threshold)
    {
        return new MinMaxedRangePredicate(false, threshold);
    }
}
