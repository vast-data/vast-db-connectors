/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.predicate.in;

import org.apache.spark.sql.types.DataType;

import java.util.Map;

import static com.vastdata.client.VastConfig.DYNAMIC_FILTER_COMPACTION_THRESHOLD;
import static com.vastdata.client.VastConfig.DYNAMIC_FILTER_COMPACTION_THRESHOLD_DEFAULT_VALUE;
import static com.vastdata.client.VastConfig.MIN_MAX_COMPACTION_MIN_VALUES_DEFAULT_VALUE;
import static com.vastdata.client.VastConfig.MIN_MAX_COMPACTION_MIN_VALUES_THRESHOLD;
import static java.lang.String.format;

public final class InHandlerResultFunctionFactory
{
    private InHandlerResultFunctionFactory() {}

    public static ResultFunction get(Map<String, Object> parsingRules, DataType dataType, ResultFunctionMethod... methods)
    {
        ResultFunction chain = null;
        for (ResultFunctionMethod method: methods) {
            ResultFunction next = getResultFunction(parsingRules, dataType, method);
            if (chain != null) {
                next.setNext(chain);
            }
            chain = next;
        }
        return chain;
    }

    private static ResultFunction getResultFunction(Map<String, Object> parsingRules, DataType dataType, ResultFunctionMethod method)
    {
        switch (method) {
            case EXPLICIT_VALUES:
                return new ExplicitValuesPredicateList();
            case MIN_MAX:
                int threshold = (int) parsingRules.getOrDefault(DYNAMIC_FILTER_COMPACTION_THRESHOLD,
                        DYNAMIC_FILTER_COMPACTION_THRESHOLD_DEFAULT_VALUE);
                return MinMaxedRangePredicate.simple(threshold);
            case MIN_MAX_FULL_RANGE_ONLY:
                return MinMaxedRangePredicate.fullRangeOnly();
            case MINIMAL_VALUE_COUNT_CHECK:
                int minValuesThreshold = (Integer) parsingRules.getOrDefault(MIN_MAX_COMPACTION_MIN_VALUES_THRESHOLD,
                        MIN_MAX_COMPACTION_MIN_VALUES_DEFAULT_VALUE);
                return new MinimalValuesCountFailBack(minValuesThreshold);
            case UNSUPPORTED_TYPE_CHECK:
                return new UnsupportedTypeFailBack(dataType);
            default:
                throw new IllegalArgumentException(format("Unknown IN predicate handling method: %s", method));
        }
    }
}
