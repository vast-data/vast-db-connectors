/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb.view;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.LeafExecNode;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.Seq;
import spark.sql.catalog.ndb.InitializedVastCatalog;
import spark.sql.catalog.ndb.VastCatalog;

import java.util.Arrays;
import java.util.Optional;

import static java.lang.String.format;

public class DropNDBViewCommand
        extends V2CommandExec
        implements LeafExecNode {
    private static final Logger LOG = LoggerFactory.getLogger(DropNDBViewCommand.class);

    // private final Seq<Attribute> columns;
    final ResolvedIdentifier resolvedIdentifier;
    final boolean ifExists;
    private Seq<SparkPlan> children_;
    final DropNDBViewPlan original;

    private DropNDBViewCommand(final ResolvedIdentifier resolvedIdentifier, final boolean ifExists, final DropNDBViewPlan original) {
        super();
        this.resolvedIdentifier = resolvedIdentifier;
        this.ifExists = ifExists;
        this.original = original;
    }


    @Override
    public Seq<InternalRow> run() {
        final VastCatalog catalog = InitializedVastCatalog.getVastCatalog();
        final boolean dropped = catalog.dropView(resolvedIdentifier.identifier(), Optional.empty());
        if (!ifExists && !dropped) {
            throw new RuntimeException(new NoSuchViewException(resolvedIdentifier.identifier()));
        }
        final InternalRow row = new GenericInternalRow(1);
        row.update(0, dropped);
        final Seq<InternalRow> result = JavaConverters.asScalaIteratorConverter(
                Arrays.stream(new InternalRow[]{row}).iterator()).asScala().toSeq();
        LOG.debug("run() returning {}", result);
        return result;
    }

    @Override
    public Seq<Attribute> output() {
        return DropNDBViewPlan.OUTPUT;
    }

    @Override
    public Seq<SparkPlan> children() {
        if (this.children_ == null) {
            return (Seq<SparkPlan>) scala.collection.immutable.Seq$.MODULE$.<SparkPlan>empty();
        } else {
            return children_.toSeq();
        }
    }

    @Override
    public SparkPlan withNewChildrenInternal(IndexedSeq<SparkPlan> newChildren) {
        this.children_ = newChildren;
        return this;
    }

    @Override
    public boolean canEqual(Object that) {
        return that instanceof DropNDBViewCommand;
    }

    @Override
    public Object productElement(int n) {
        return this;
    }

    @Override
    public int productArity() {
        return 0;
    }

    public static DropNDBViewCommand instance(final DropNDBViewPlan plan) {
        LogicalPlan child = plan.children().apply(0);
        if (child instanceof ResolvedIdentifier) {
            return new DropNDBViewCommand((ResolvedIdentifier) child, plan.ifExists, plan);
        }
        throw new RuntimeException(format("Unexpected child class: %s for drop view plan: %s", child.getClass(), plan));
    }
}
