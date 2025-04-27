/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.predicate.in;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinimalValuesCountFailBack extends ExplicitValuesPredicateList
{
    private static final Logger LOG = LoggerFactory.getLogger(MinimalValuesCountFailBack.class);

    private final int minValuesThreshold;

    public MinimalValuesCountFailBack(int minValuesThreshold) {
        super();
        this.minValuesThreshold = minValuesThreshold;
    }

    @Override
    public Predicate[] apply(ProcessedInValues processedInValues, Expression[] values)
    {
        if (values.length > minValuesThreshold)
        {
            return applyNext(processedInValues, values);
        }
        else {
            LOG.info("Value count: {} is below optimization min values threshold: {}", values.length - 1, minValuesThreshold);
            return super.apply(processedInValues, values);
        }
    }
}
