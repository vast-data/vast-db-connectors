/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb.view;

import org.apache.spark.sql.catalyst.analysis.UnresolvedTableOrView;
import org.apache.spark.sql.catalyst.analysis.UnresolvedView;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.AlterViewAs;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.Function1;
import scala.PartialFunction;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.Seq;

import static ndb.NDBParser.EMPTY_LOGICAL_PLAN_SEQ;

public class AlterNDBViewAsPlan
        extends LogicalPlan
{
    private final String originalText;
    private Seq<LogicalPlan> children;

    private AlterNDBViewAsPlan(final AlterViewAs original)
    {
        super();
        originalText = original.originalText();
        this.children = (Seq<LogicalPlan>) original.children().toSeq();
    }

    @Override
    public Seq<Attribute> output()
    {
        return (Seq<Attribute>) scala.collection.immutable.Seq$.MODULE$.<Attribute>empty();
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
        return that instanceof AlterNDBViewAsPlan;
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

    public static AlterNDBViewAsPlan instance(final AlterViewAs plan)
    {
        Function1<LogicalPlan, LogicalPlan> resolveViewFunc = p -> {
            if (p instanceof UnresolvedView) {
                UnresolvedView uv = (UnresolvedView) p;
                return new UnresolvedTableOrView(uv.multipartIdentifier(), uv.commandName(), uv.allowTemp());
            }
            else {
                return p;
            }
        };
        PartialFunction<LogicalPlan, LogicalPlan> transformer = PartialFunction.fromFunction(resolveViewFunc);
        return new AlterNDBViewAsPlan((AlterViewAs) plan.resolveOperators(transformer));
    }

    @Override
    public boolean resolved()
    {
        return childrenResolved();
    }

    public String getOriginalText()
    {
        return originalText;
    }
}
