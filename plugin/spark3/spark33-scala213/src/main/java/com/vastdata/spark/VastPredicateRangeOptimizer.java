/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.vastdata.spark.predicate.VastPredicate;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static java.lang.String.format;

public class VastPredicateRangeOptimizer implements Function<List<VastPredicate>, List<VastPredicate>>
{
    private static final Logger LOG = LoggerFactory.getLogger(VastPredicateRangeOptimizer.class);

    private static final Set<DataType> fullRangeMinMaxOptimizationTypes = ImmutableSet.of(LongType$.MODULE$, IntegerType$.MODULE$, DateType$.MODULE$);

    public static boolean isAllowedTypeForRangeOptimization(DataType dataType)
    {
        boolean allowed = fullRangeMinMaxOptimizationTypes.contains(dataType);
        if (!allowed) {
            LOG.warn("Range optimization is not allowed for type: {}", dataType);
        }
        return allowed;
    }

    @Override
    public List<VastPredicate> apply(List<VastPredicate> vastPredicatesList)
    {
        LiteralValue<?> min = null;
        LiteralValue<?> max = null;
        NamedReference firstColRef = null;
        for (VastPredicate vp: vastPredicatesList) {
            Predicate predicate = vp.getPredicate();
            NamedReference currRef = vp.getReference();
            if (firstColRef == null) {
                firstColRef = currRef;
            }
            else if (!firstColRef.equals(currRef)) {
                LOG.warn("Returning null because of predicate column is not the same for all list entries: current: {}, previous: {}", currRef, firstColRef);
            }
            if (predicate.name().equals("=")) {
                Expression[] children = predicate.children();
                LiteralValue<?> child = (LiteralValue<?>) children[1];
                Object value = child.value();
                DataType dataType = vp.getField().dataType();
                if (!isValueValid(dataType, value)) { // null and NaN values are not supported
                    LOG.warn("Returning null because of predicate value is invalid: {}", predicate);
                    return null;
                }
                boolean compareMax = true;
                if (min == null) {
                    min = child;
                }
                else {
                    if (compare(dataType, value, min.value()) < 0) {
                        min = child;
                        compareMax = false;
                    }
                }
                if (max == null) {
                    max = child;
                }
                else if(compareMax && compare(vp.getField().dataType(), value, max.value()) > 0) {
                    max = child;
                }
            }
            else {
                LOG.warn("Returning null because of predicate is not of equality type: {}", predicate);
                return null; // expecting only OR of "=" list
            }
        }
        if (min == null) { // if min was set, max is set too
            LOG.warn("Returning null because of unset values");
            return null;
        }
        StructField field = vastPredicatesList.get(0).getField();
        VastPredicate minPredicate = new VastPredicate(new Predicate(">=", new Expression[] {firstColRef, min}), firstColRef, field);
        VastPredicate maxPredicate = new VastPredicate(new Predicate("<=", new Expression[] {firstColRef, max}), firstColRef, field);
        ImmutableList<VastPredicate> optimized = ImmutableList.of(minPredicate, maxPredicate);
        LOG.info("Returning new range predicates: {}", optimized);
        return optimized;
    }

    private boolean isValueValid(DataType dataType, Object value)
    {
        if (value == null) {
            return false;
        }
        if (dataType instanceof org.apache.spark.sql.types.LongType) {
            return true;
        }
        else if (dataType instanceof org.apache.spark.sql.types.IntegerType) {
            return true;
        }
        else if (dataType instanceof org.apache.spark.sql.types.DoubleType) {
            return !Double.isNaN((Double) value);
        }
        else if (dataType instanceof org.apache.spark.sql.types.FloatType) {
            return !Float.isNaN((Float) value);
        }
        throw new IllegalArgumentException(format("Unexpected type: %s", dataType));
    }

    private int compare(DataType dataType, Object val1, Object val2)
    {
        if (dataType instanceof org.apache.spark.sql.types.LongType) {
            return Long.compare((Long) val1, (Long) val2);
        }
        else if (dataType instanceof org.apache.spark.sql.types.IntegerType) {
            return Integer.compare((Integer) val1, (Integer) val2);
        }
        else if (dataType instanceof org.apache.spark.sql.types.DoubleType) {
            return Double.compare((Double) val1, (Double) val2);
        }
        else if (dataType instanceof org.apache.spark.sql.types.FloatType) {
            return Float.compare((Float) val1, (Float) val2);
        }
        throw new IllegalArgumentException(format("Unexpected type: %s", dataType));
    }
}
