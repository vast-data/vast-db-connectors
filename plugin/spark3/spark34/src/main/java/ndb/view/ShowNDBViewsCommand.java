/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb.view;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.UnresolvedNamespace;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits$;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.LeafExecNode;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.Seq;
import spark.sql.catalog.ndb.InitializedVastCatalog;
import spark.sql.catalog.ndb.VastCatalog;

import java.util.Arrays;

public class ShowNDBViewsCommand
        extends V2CommandExec
        implements LeafExecNode
{
    private static final Logger LOG = LoggerFactory.getLogger(ShowNDBViewsCommand.class);

    private IndexedSeq<SparkPlan> children = null;
    final ShowNDBViewsPlan original;

    private ShowNDBViewsCommand(final ShowNDBViewsPlan original)
    {
        super();
        this.original = original;
    }

    private InternalRow identifierToRow(final Identifier identifier)
    {
        final String namespace = CatalogV2Implicits$.MODULE$.NamespaceHelper(identifier.namespace()).quoted();
        final String name = identifier.name();
        final boolean isTemp = session().sessionState().catalog().isTempView(
                TableIdentifier.apply(
                        identifier.name(),
                        Option.apply(session().catalog().currentDatabase()),
                        Option.apply(session().sessionState().catalogManager().currentCatalog().name())
                )
        );

        final InternalRow row = new GenericInternalRow(3);
        row.update(0, UTF8String.fromString(namespace));
        row.update(1, UTF8String.fromString(name));
        row.setBoolean(2, isTemp);
        return row;
    }

    @Override
    public Seq<InternalRow> run()
    {
        final VastCatalog catalog = InitializedVastCatalog.getVastCatalog();
        try {
            final String[] prefix = ((UnresolvedNamespace) original.original.child()).multipartIdentifier().drop(1).mkString(".").split("\\.");
            final Identifier[] views = catalog.listViews(prefix);
            final Seq<InternalRow> result = JavaConverters.asScalaIteratorConverter(
                    Arrays.stream(views).map(this::identifierToRow).iterator()).asScala().toSeq();
            LOG.debug("Returning result: {}", result);
            return result;
        } catch (final NoSuchNamespaceException error) {
            throw new RuntimeException(error);
        }
    }

    @Override
    public Seq<Attribute> output()
    {
        return original.output();
    }

    @Override
    public Seq<SparkPlan> children()
    {
        if (this.children == null) {
            return (Seq<SparkPlan>) scala.collection.immutable.Seq$.MODULE$.<SparkPlan>empty();
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
        return that instanceof ShowNDBViewsCommand;
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

    public static ShowNDBViewsCommand instance(ShowNDBViewsPlan plan)
    {
        return new ShowNDBViewsCommand(plan);
    }
}
