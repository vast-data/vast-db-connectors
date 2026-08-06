/*
 *  Copyright (C) Vast Data Ltd.
 */
package spark.sql.catalog.ndb;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;

public class NamedTransform
        implements Transform
{
    final String n;
    final Expression[] children;

    public NamedTransform(String n, String col)
    {
        this.n = n;
        children = new Expression[] {Expressions.column(col)};
    }

    @Override
    public String name()
    {
        return n;
    }

    @Override
    public Expression[] arguments()
    {
        return children;
    }

    @Override
    public String toString()
    {
        return n + "(" + children[0].describe() + ")";
    }
}
