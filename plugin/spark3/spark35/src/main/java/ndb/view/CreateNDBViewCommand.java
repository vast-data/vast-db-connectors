/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb.view;

import com.vastdata.spark.SparkViewMetadata;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.CreateView;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.LeafExecNode;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.Seq;
import scala.collection.immutable.Seq$;
import spark.sql.catalog.ndb.InitializedVastCatalog;
import spark.sql.catalog.ndb.VastCatalog;

import java.util.HashMap;
import java.util.Optional;

public class CreateNDBViewCommand
        extends V2CommandExec
        implements LeafExecNode
{
    private static final Logger LOG = LoggerFactory.getLogger(CreateNDBViewCommand.class);
    private Seq<SparkPlan> children = (Seq<SparkPlan>) Seq$.MODULE$.<SparkPlan>empty();
    private final SparkViewMetadata sparkViewMetadata;

    private CreateNDBViewCommand(SparkViewMetadata sparkViewMetadata) {
        super();
        this.sparkViewMetadata = sparkViewMetadata;
    }

    @Override
    public Seq<InternalRow> run()
    {
        final java.util.Map<String, String> properties2 = new HashMap<>();
        sparkViewMetadata.getProperties().foreach(pair -> properties2.put(pair._1(), pair._2()));
        try {
            final VastCatalog catalog = InitializedVastCatalog.getVastCatalog();
            catalog.createView(sparkViewMetadata, sparkViewMetadata.isReplace(), Optional.empty());
            LOG.debug("Created view successfully");
        } catch (final ViewAlreadyExistsException | NoSuchNamespaceException error) {
            throw new RuntimeException(error);
        }
        return (Seq<InternalRow>) Seq$.MODULE$.<InternalRow>empty();
    }

    @Override
    public Seq<Attribute> output()
    {
        return (Seq<Attribute>) Seq$.MODULE$.<Attribute>empty();
    }

    @Override
    public Seq<SparkPlan> children()
    {
        if (this.children == null) {
            return (Seq<SparkPlan>) Seq$.MODULE$.<SparkPlan>empty();
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
        return that instanceof CreateNDBViewCommand;
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

    public static CreateNDBViewCommand instance(final CreateNDBViewPlan plan)
    {
        LOG.debug("CreateNDBViewCommand instance from plan: {}", plan);
        Seq<LogicalPlan> children = plan.children();
        ResolvedIdentifier resolvedIdentifier = (ResolvedIdentifier) children.apply(0);
        LogicalPlan queryPlan = children.apply(1);
        Identifier identifier = resolvedIdentifier.identifier();
        CreateView originalCreateView = plan.getOriginal();
        boolean replace = originalCreateView.replace();
        boolean allowExisting = originalCreateView.allowExisting();
        String origRawViewSqlDefinition = originalCreateView.originalText().get();
        SparkViewMetadata sparkViewMetadata = new SparkViewMetadata(
                identifier, replace, allowExisting, originalCreateView.comment(),
                originalCreateView.userSpecifiedColumns(), originalCreateView.properties(), origRawViewSqlDefinition,
                queryPlan.schema(), plan.getCurrentCatalog(), plan.getCurrentNamespace());
        LOG.debug("CreateNDBViewCommand sparkViewMetadata: {}", sparkViewMetadata);
        return new CreateNDBViewCommand(sparkViewMetadata);
    }
}
