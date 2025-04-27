/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.ResolvedTable;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ShowColumns;
import org.apache.spark.sql.execution.LeafExecNode;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.Seq;
import scala.collection.mutable.Builder;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.lang.String.format;

public class ShowNDBTableColumnsCommand
        extends V2CommandExec
        implements LeafExecNode
{
    private static final Logger LOG = LoggerFactory.getLogger(ShowNDBTableColumnsCommand.class);
    private static final Function<Attribute, StructField> ATTRIBUTE_STRUCT_FIELD_FUNCTION = att -> new StructField(att.name(), att.dataType(), att.nullable(), att.metadata());
    private static final BiConsumer<StructField, Builder<InternalRow, List<InternalRow>>> INTERNAL_ROW_TRANSFORMATOR = (structField, builder) -> {
        GenericInternalRow genericInternalRow = new GenericInternalRow(4);
        genericInternalRow.update(0, UTF8String.fromString(structField.name()));
        genericInternalRow.update(1, structField.dataType());
        genericInternalRow.update(2, structField.nullable());
        genericInternalRow.update(3, structField.metadata());
        builder.$plus$eq(genericInternalRow);
    };
    private final Seq<Attribute> columns;
    private IndexedSeq<SparkPlan> children = null;

    private ShowNDBTableColumnsCommand(Seq<Attribute> attributeSeq) {
        super();
        this.columns = attributeSeq;
    }

    @Override
    public scala.collection.immutable.Seq<InternalRow> run()
    {
        Builder<InternalRow, List<InternalRow>> builder = List$.MODULE$.newBuilder();
        IntStream.range(0, columns.size())
                .mapToObj(columns::apply)
                .map(ATTRIBUTE_STRUCT_FIELD_FUNCTION)
                .forEach(getStructFieldConsumer(builder));
        scala.collection.immutable.Seq<InternalRow> result = builder.result();
        LOG.debug("run() returning {}", result);
        return result;
    }

    @NotNull
    private static Consumer<StructField> getStructFieldConsumer(Builder<InternalRow, List<InternalRow>> builder)
    {
        return field -> INTERNAL_ROW_TRANSFORMATOR.accept(field, builder);
    }

    @Override
    public scala.collection.immutable.Seq<Attribute> output()
    {
        return (scala.collection.immutable.Seq<Attribute>) scala.collection.immutable.Seq$.MODULE$.<Attribute>empty();
    }

    @Override
    public scala.collection.immutable.Seq<SparkPlan> children()
    {
        if (this.children == null) {
            return (scala.collection.immutable.Seq<SparkPlan>) scala.collection.immutable.Seq$.MODULE$.<SparkPlan>empty();
        }
        else {
            return children.toSeq();
        }
    }

    @Override
    public SparkPlan withNewChildrenInternal(IndexedSeq<SparkPlan> newChildren)
    {
        this.children = newChildren;
        return this;
    }

    @Override
    public boolean canEqual(Object that)
    {
        return that instanceof ShowNDBTableColumnsCommand;
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

    public static ShowNDBTableColumnsCommand instance(ShowColumns plan)
    {
        LogicalPlan child = plan.child();
        if (child instanceof ResolvedTable) {
            ResolvedTable resolvedTable = (ResolvedTable) child;
            return new ShowNDBTableColumnsCommand(resolvedTable.outputAttributes());
        }
        else {
            throw new RuntimeException(format("Unexpected child plan type: %s", plan.toJSON()));
        }
    }
}
