/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb.view;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ShowViews;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.Seq;


public class ShowNDBViewsPlan extends LogicalPlan
{
    private IndexedSeq<LogicalPlan> children = null;
    final ShowViews original;
    final Seq<Attribute> cachedOutput;

    private ShowNDBViewsPlan(final ShowViews original) {
        super();
        this.original = original;
        cachedOutput = original.getOutputAttrs();
    }

    @Override
    public Seq<Attribute> output()
    {
        return cachedOutput;
    }

    @Override
    public Seq<LogicalPlan> children()
    {
        if (this.children == null) {
            return (Seq<LogicalPlan>) scala.collection.immutable.Seq$.MODULE$.<LogicalPlan>empty();
        }
        else {
            return children.toSeq();
        }
    }

    // TODO: these `withX` methods should return a modified *copy*
    @Override
    public LogicalPlan withNewChildrenInternal(IndexedSeq<LogicalPlan> newChildren) {
        {
            this.children = newChildren;
            return this;
        }
    }

    @Override
    public boolean canEqual(Object that)
    {
        return that instanceof ShowNDBViewsPlan;
    }

    @Override
    public Object productElement(int n)
    {
        return this;
    }

    @Override
    public int productArity()
    {
        return 0;
    }

    public static ShowNDBViewsPlan instance(ShowViews plan)
    {
        return new ShowNDBViewsPlan(plan);
    }
}
