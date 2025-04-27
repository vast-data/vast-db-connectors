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

import java.util.Arrays;
import java.util.Optional;

import static java.lang.String.format;

public class RenameNDBViewCommand
        extends V2CommandExec
        implements LeafExecNode {
    private final SparkSession session;
    final ResolvedPersistentView resolvedView;
    private final String newName;
    private Seq<SparkPlan> children = (Seq<SparkPlan>) Seq$.MODULE$.<SparkPlan>empty();

    private RenameNDBViewCommand(SparkSession session, ResolvedPersistentView resolvedView, String newName) {
        super();
        this.session = session;
        this.resolvedView = resolvedView;
        this.newName = newName;
    }


    @Override
    public Seq<InternalRow> run() {
        final VastCatalog catalog = InitializedVastCatalog.getVastCatalog();
        Identifier oldIdentifier = resolvedView.identifier();
        try {
            VastClient vastClient = NDB.getVastClient(NDB.getConfig());
            VastSparkTransactionsManager transactionsManager = VastSparkTransactionsManager.getInstance(vastClient, new VastSimpleTransactionFactory());
            try (final VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
                try {
                    Optional<VastTransaction> txOptional = Optional.of(tx);
                    VastView vastView = catalog.loadView(oldIdentifier, txOptional);
                    Identifier newIdentifier = Identifier.of(oldIdentifier.namespace(), newName);
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
                    StructType structType = session.sql(vastView.query()).logicalPlan().schema();
                    SparkViewMetadata ctx = new SparkViewMetadata(newIdentifier, false, false,
                            Option.apply(comment), propsScalaMap, vastView.query(), structType,
                            vastView.currentCatalog(), vastView.currentNamespace(), columnComments, columnAliases);
                    catalog.dropView(oldIdentifier, txOptional);
                    catalog.createView(ctx, false, txOptional);
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
    public Seq<Attribute> output() {
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
    public boolean canEqual(Object that) {
        return that instanceof RenameNDBViewCommand;
    }

    @Override
    public Object productElement(int n) {
        return this;
    }

    @Override
    public int productArity() {
        return 0;
    }

    public static RenameNDBViewCommand instance(final RenameNDBViewPlan plan, SparkSession session) {
        LogicalPlan child = plan.children().apply(0);
        if (child instanceof ResolvedPersistentView) {
            ResolvedPersistentView resolvedPersistentView = (ResolvedPersistentView) child;
            Seq<String> newNameSeq = plan.original.newName();
            String newName;
            if (newNameSeq.size() == 1) {
                newName = newNameSeq.apply(0);
            }
            else {
                String[] namespace = new String[newNameSeq.size() - 1];
                for (int i = 0; i < namespace.length; i++) {
                    namespace[i] = newNameSeq.apply(i);
                }
                if (!Arrays.equals(namespace, resolvedPersistentView.identifier().namespace())) {
                    throw new RuntimeException(format("Rename NDB view can not change view schema path. current: %s, new: %s",
                            Arrays.toString(resolvedPersistentView.identifier().namespace()), Arrays.toString(namespace)));
                }
                newName = newNameSeq.apply(newNameSeq.size() - 1);
            }
            return new RenameNDBViewCommand(session, resolvedPersistentView, newName);
        }
        throw new RuntimeException(format("Unexpected child class: %s for drop view plan: %s", child.getClass(), plan));
    }
}
