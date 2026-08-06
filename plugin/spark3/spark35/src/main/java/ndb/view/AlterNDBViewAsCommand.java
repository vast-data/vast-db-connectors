/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb.view;

import com.vastdata.client.VastClient;
import com.vastdata.client.tx.VastAutocommitTransaction;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.client.tx.VastTransactionFactory;
import com.vastdata.spark.SparkViewMetadata;
import com.vastdata.spark.VastView;
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
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.Map;
import scala.collection.immutable.Seq;
import spark.sql.catalog.ndb.InitializedVastCatalog;
import spark.sql.catalog.ndb.VastCatalog;

import java.util.Optional;

import static com.vastdata.spark.SparkPlannerUtil.getEmptyAttributeSeq;
import static com.vastdata.spark.SparkPlannerUtil.getEmptyInternalRowSeq;
import static com.vastdata.spark.SparkPlannerUtil.getEmptySparkPlanSeq;
import static com.vastdata.spark.SparkPlannerUtil.getViewPropertiesMap;

public class AlterNDBViewAsCommand
        extends V2CommandExec
        implements LeafExecNode
{
    private final SparkSession session;
    private final String originalText;
    private final LogicalPlan query;
    private final ResolvedPersistentView resolvedView;
    private Seq<SparkPlan> children = getEmptySparkPlanSeq();

    private AlterNDBViewAsCommand(SparkSession session, String originalText,
            LogicalPlan query, ResolvedPersistentView resolvedView)
    {
        super();
        this.session = session;
        this.originalText = originalText;
        this.query = query;
        this.resolvedView = resolvedView;
    }

    public static AlterNDBViewAsCommand instance(final AlterNDBViewAsPlan plan,
            SparkSession session)
    {
        return new AlterNDBViewAsCommand(session, plan.getOriginalText(),
                plan.children().apply(1),
                (ResolvedPersistentView) plan.children().apply(0));
    }

    @Override
    public Seq<InternalRow> run()
    {
        final String endUser = null;
        final VastCatalog catalog = InitializedVastCatalog.getVastCatalog();
        Identifier viewIdentifier = resolvedView.identifier();
        try {
            VastClient vastClient = NDB.getVastClient(NDB.getConfig());
            VastSparkTransactionsManager transactionsManager = VastSparkTransactionsManager.getInstance(
                    vastClient, new VastTransactionFactory());
            try (final VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                    transactionsManager,
                    () -> transactionsManager.startTransaction(endUser),
                    endUser)) {
                try {
                    Optional<VastTransaction> txToUse = Optional.of(tx);
                    VastView vastView = catalog.loadView(viewIdentifier,
                            txToUse);

                    String comment = vastView.properties().get("comment");
                    String[] columnAliases = vastView.columnAliases();
                    String[] columnComments = vastView.columnComments();
                    Map<String, String> propsScalaMap = getViewPropertiesMap(
                            vastView);
                    String currentCatalog = vastView.currentCatalog();
                    String[] currentNamespace = vastView.currentNamespace();

                    StructType newSchema = this.query.schema();
                    // schema changes, but spark doesn't allow setting new aliases and comments, so it has to be reset, otherwise view resolution errors might happen
                    StructType oldSchema = session
                            .sql(vastView.query())
                            .logicalPlan()
                            .schema();
                    if (!newSchema.equals(oldSchema)) {
                        columnAliases = new String[0];
                        columnComments = new String[0];
                    }

                    SparkViewMetadata ctx = new SparkViewMetadata(
                            viewIdentifier, false, false, Option.apply(comment),
                            propsScalaMap, originalText, newSchema,
                            currentCatalog, currentNamespace, columnComments,
                            columnAliases);

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
        return getEmptyInternalRowSeq();
    }

    @Override
    public Seq<Attribute> output()
    {
        return getEmptyAttributeSeq();
    }

    @Override
    public Seq<SparkPlan> children()
    {
        if (this.children == null) {
            return getEmptySparkPlanSeq();
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
}
