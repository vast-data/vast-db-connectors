/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.predicate.in;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vastdata.spark.VastPredicateRangeOptimizer.isAllowedTypeForRangeOptimization;

public class UnsupportedTypeFailBack extends ExplicitValuesPredicateList
{
    private static final Logger LOG = LoggerFactory.getLogger(UnsupportedTypeFailBack.class);
    private final DataType dataType;

    public UnsupportedTypeFailBack(DataType dataType) {
        this.dataType = dataType;
    }

    @Override
    public Predicate[] apply(ProcessedInValues minMaxResult, Expression[] values)
    {
        if (isAllowedTypeForRangeOptimization(dataType)) {
            return applyNext(minMaxResult, values);
        }
        else {
            LOG.info("Unsupported type for optimization: {}", dataType);
            return super.apply(minMaxResult, values);
        }
    }
}
