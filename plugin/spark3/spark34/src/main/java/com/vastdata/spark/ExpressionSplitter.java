/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.PredicateHelper;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy;
import scala.collection.Seq;

import java.util.function.Function;

public final class ExpressionSplitter
{
    private static final PredicateHelper DATA_SOURCE_V_2_STRATEGY = new DataSourceV2Strategy(null);
    private static final Function<Expression, Seq<Expression>> SPLIT_EXPRESSION = DATA_SOURCE_V_2_STRATEGY::splitConjunctivePredicates;

    private ExpressionSplitter() {}

    public static Seq<Expression> split(Expression e)
    {
        return SPLIT_EXPRESSION.apply(e);
    }
}
