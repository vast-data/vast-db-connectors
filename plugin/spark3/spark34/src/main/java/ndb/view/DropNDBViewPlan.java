/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb.view;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.DropView;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.List;
import scala.collection.immutable.Seq;
import scala.collection.mutable.Builder;

import static ndb.NDBParser.EMPTY_LOGICAL_PLAN_SEQ;

public class DropNDBViewPlan
        extends LogicalPlan
{
    public static final Seq<Attribute> OUTPUT;
    static {
        Builder<Attribute, List<Attribute>> b = List.newBuilder();
        Attribute resAttr = new AttributeReference("dropped",
                DataTypes.BooleanType, true, Metadata.empty(), ExprId.apply(0),
                (scala.collection.immutable.Seq<String>) scala.collection.immutable.Seq$.MODULE$.<String>empty());
        b.addOne(resAttr);
        OUTPUT = b.result();
    }
    private Seq<LogicalPlan> children;
    final boolean ifExists;
    final DropView original;

    private DropNDBViewPlan(final boolean ifExists,
                            final DropView original) {
        super();
        this.ifExists = ifExists;
        this.original = original;
        this.children = (Seq<LogicalPlan>) original.children().toSeq();
    }

    @Override
    public Seq<Attribute> output()
    {
        return OUTPUT;
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
        return that instanceof DropNDBViewPlan;
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

    public static DropNDBViewPlan instance(final DropView plan)
    {
        return new DropNDBViewPlan(plan.ifExists(), plan);
    }
}
