/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.predicate;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.LogicalExpressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataType;

public final class VastPredicates
{
    public static class Equal
            extends Predicate
    {
        public Equal(NamedReference col, DataType t, Object v)
        {
            super("=",
                    new Expression[] {col, LogicalExpressions.literal(v, t)});
        }
    }

    public static class IsNull
            extends Predicate
    {
        public IsNull(NamedReference col)
        {
            super("IS_NULL", new Expression[] {col});
        }
    }
}
