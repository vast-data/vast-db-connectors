/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb.view;

import com.vastdata.client.VastClient;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.spark.SparkViewMetadata;
import com.vastdata.spark.VastView;
import com.vastdata.spark.tx.VastAutocommitTransaction;
import com.vastdata.spark.tx.VastSimpleTransactionFactory;
import com.vastdata.spark.tx.VastSparkTransactionsManager;
import ndb.NDB;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.ResolvedPersistentView;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.LeafExecNode;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.Map;
import scala.collection.immutable.Seq;
import scala.collection.immutable.Seq$;
import scala.collection.mutable.Builder;
import spark.sql.catalog.ndb.InitializedVastCatalog;
import spark.sql.catalog.ndb.VastCatalog;

import java.util.Optional;

public class AlterNDBViewAsCommand
        extends V2CommandExec
        implements LeafExecNode
{
    private final SparkSession session;
    private final String originalText;
    private final LogicalPlan query;
    private final ResolvedPersistentView resolvedView;
    private Seq<SparkPlan> children = (Seq<SparkPlan>) Seq$.MODULE$.<SparkPlan>empty();

    private AlterNDBViewAsCommand(SparkSession session, String originalText, LogicalPlan query, ResolvedPersistentView resolvedView)
    {
        super();
        this.session = session;
        this.originalText = originalText;
        this.query = query;
        this.resolvedView = resolvedView;
    }


    @Override
    public Seq<InternalRow> run()
    {
        final VastCatalog catalog = InitializedVastCatalog.getVastCatalog();
        Identifier viewIdentifier = resolvedView.identifier();
        try {
            VastClient vastClient = NDB.getVastClient(NDB.getConfig());
            VastSparkTransactionsManager transactionsManager = VastSparkTransactionsManager.getInstance(vastClient, new VastSimpleTransactionFactory());
            try (final VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
                try {
                    Optional<VastTransaction> txToUse = Optional.of(tx);
                    VastView vastView = catalog.loadView(viewIdentifier, txToUse);

                    String comment = vastView.properties().get("comment");
                    String[] columnAliases = vastView.columnAliases();
                    String[] columnComments = vastView.columnComments();
                    Builder<Tuple2<String, String>, Map<String, String>> mapBuilder = Map.newBuilder();
                    java.util.Map<String, String> currentProperties = vastView.properties();
                    currentProperties.entrySet().stream()
                            .filter(e -> !e.getKey().equals("comment"))
                            .map(e -> Tuple2.apply(e.getKey(), e.getValue()))
                            .forEach(mapBuilder::addOne);
                    Map<String, String> propsScalaMap = mapBuilder.result();
                    String currentCatalog = vastView.currentCatalog();
                    String[] currentNamespace = vastView.currentNamespace();

                    StructType newSchema = this.query.schema();
                    // schema changes, but spark doesn't allow setting new aliases and comments, so it has to be reset, otherwise view resolution errors might happen
                    StructType oldSchema = session.sql(vastView.query()).logicalPlan().schema();
                    if (!newSchema.equals(oldSchema)) {
                        columnAliases = new String[0];
                        columnComments = new String[0];
                    }

                    SparkViewMetadata ctx = new SparkViewMetadata(viewIdentifier, false, false,
                            Option.apply(comment), propsScalaMap, originalText, newSchema,
                            currentCatalog, currentNamespace, columnComments, columnAliases);

                    catalog.dropView(viewIdentifier, txToUse);
                    catalog.createView(ctx, false, txToUse);
                }
                catch (Exception any) {
                    tx.setCommit(false);
                    throw any;
                }
            }
            catch (NoSuchViewException e) {
                throw new RuntimeException(e);
            }
        }
        catch (Exception any) {
            throw new RuntimeException("Failed altering view", any);
        }
        return (Seq<InternalRow>) Seq$.MODULE$.empty();
    }

    @Override
    public Seq<Attribute> output()
    {
        return (Seq<Attribute>) scala.collection.immutable.Seq$.MODULE$.<Attribute>empty();
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
        return that instanceof AlterNDBViewAsCommand;
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

    public static AlterNDBViewAsCommand instance(final AlterNDBViewAsPlan plan, SparkSession session)
    {
        return new AlterNDBViewAsCommand(session, plan.getOriginalText(),
                plan.children().apply(1), (ResolvedPersistentView) plan.children().apply(0));
    }
}
