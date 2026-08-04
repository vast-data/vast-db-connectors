/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb.view;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ShowViews;
import scala.collection.IndexedSeq;
import scala.collection.Seq;

import static ndb.SparkPlannerUtil.EMPTY_LOGICAL_PLAN_SEQ;

public class ShowNDBViewsPlan
        extends LogicalPlan
{
    final ShowViews original;
    final Seq<Attribute> cachedOutput;
    private IndexedSeq<LogicalPlan> children = null;

    private ShowNDBViewsPlan(final ShowViews original)
    {
        super();
        this.original = original;
        cachedOutput = ShowViews.getOutputAttrs();
    }

    public static ShowNDBViewsPlan instance(ShowViews plan)
    {
        return new ShowNDBViewsPlan(plan);
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
            return EMPTY_LOGICAL_PLAN_SEQ;
        }
        else {
            return children.toSeq();
        }
    }

    // TODO: these `withX` methods should return a modified *copy*
    @Override
    public LogicalPlan withNewChildrenInternal(
            IndexedSeq<LogicalPlan> newChildren)
    {
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
}
