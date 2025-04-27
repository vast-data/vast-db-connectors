/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb.view;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.CreateView;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.Seq;

import static ndb.NDBParser.EMPTY_LOGICAL_PLAN_SEQ;

public class CreateNDBViewPlan
        extends LogicalPlan
{
    private final CreateView original;
    private final String currentCatalog;
    private final String[] currentNamespace;
    private Seq<LogicalPlan> children;


    private CreateNDBViewPlan(final CreateView original, String currentCatalog, String[] currentNamespace) {
        super();
        this.original = original;
        this.currentCatalog = currentCatalog;
        this.currentNamespace = currentNamespace;
        this.children = (Seq<LogicalPlan>) original.children().toSeq();
    }

    public CreateView getOriginal()
    {
        return original;
    }

    @Override
    public boolean canEqual(Object that)
    {
        return that instanceof CreateNDBViewPlan;
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
    public Seq<Attribute> output()
    {
        return (Seq<Attribute>) scala.collection.immutable.Seq$.MODULE$.<Attribute>empty();
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

    public static CreateNDBViewPlan instance(final CreateView plan, String currentCatalog, String[] currentNamespace)
    {
        return new CreateNDBViewPlan(plan, currentCatalog, currentNamespace);
    }

    public String getCurrentCatalog()
    {
        return currentCatalog;
    }

    public String[] getCurrentNamespace()
    {
        return currentNamespace;
    }

    @Override
    public boolean resolved()
    {
        return childrenResolved();
    }
}
