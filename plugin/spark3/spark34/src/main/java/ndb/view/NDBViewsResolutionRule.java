package ndb.view;

import com.vastdata.spark.VastView;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.AliasIdentifier;
import org.apache.spark.sql.catalyst.analysis.ResolvedPersistentView;
import org.apache.spark.sql.catalyst.analysis.ResolvedTable;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.analysis.UnresolvedTableOrView;
import org.apache.spark.sql.catalyst.analysis.UnresolvedView;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.errors.QueryCompilationErrors;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Option;
import scala.PartialFunction;
import scala.collection.immutable.ArraySeq;
import scala.collection.immutable.ArraySeq$;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.Seq;
import scala.collection.immutable.Seq$;
import scala.collection.mutable.Builder;
import spark.sql.catalog.ndb.InitializedVastCatalog;
import spark.sql.catalog.ndb.VastCatalog;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.String.format;
import static spark.sql.catalog.ndb.NDBRowLevelOperationIdentifier.isForRowLevelOp;

public class NDBViewsResolutionRule
        extends Rule<LogicalPlan>
{
    private static final Logger LOG = LoggerFactory.getLogger(NDBViewsResolutionRule.class);
    public static final Seq<String> EMPTY_STRING_SEQ = (Seq<String>) Seq$.MODULE$.empty();
    private final SparkSession session;
    private VastCatalog vastCatalog = null;

    public NDBViewsResolutionRule(SparkSession session)
    {
        this.session = session;
    }

    private static final Function<LogicalPlan, Supplier<Seq<String>>> unresolvedIdentifierSeqSupplier = p -> {
           if (p instanceof UnresolvedRelation) {
               return ((UnresolvedRelation) p)::multipartIdentifier;
           }
           else if (p instanceof UnresolvedTableOrView) {
               return ((UnresolvedTableOrView) p)::multipartIdentifier;
           }
           else if (p instanceof UnresolvedView) {
               return ((UnresolvedView) p)::multipartIdentifier;
           }
           else throw new RuntimeException("Unexpected class for unresolved identifier resolution: " + p.getClass());
    };

    BiFunction<Seq<String>, String[], VastView> viewLoader = (uRelName, currentNamespace) -> {
        String[] namespaceForLookup;
        try {
            namespaceForLookup = new VastNamespaceResolver().apply(uRelName, currentNamespace);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to resolve namespace for view: " + uRelName, e);
        }
        Identifier ident = Identifier.of(namespaceForLookup, uRelName.last());
        LOG.debug("Trying to load view for identifier {}", ident);
        try {
            VastView vastView = getVastCatalog().loadView(ident, Optional.empty());
            LOG.debug("Loaded view: {}", vastView);
            return vastView;
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to load view for identifier: " + ident, e);
        }
    };

    @Override
    public LogicalPlan apply(LogicalPlan plan)
    {
        if (plan instanceof DropNDBViewPlan ||
                plan instanceof ShowNDBViewsPlan) {
            return plan;
        }
        Function1<LogicalPlan, LogicalPlan> resolveViewFunc = p -> {
            if (p instanceof UnresolvedRelation) {
                LOG.debug("Trying to resolve UnresolvedRelation: {}", p);
                Seq<String> uRelName = unresolvedIdentifierSeqSupplier.apply(p).get();
                String[] currentNamespace = session.sessionState().catalogManager().currentNamespace();
                if (isForRowLevelOp(uRelName.last())) {
                    LOG.info("Skipping resolution for row-level-op UnresolvedRelation: {}", uRelName);
                    return p;
                }
                VastView vastView;
                try {
                    vastView = this.viewLoader.apply(uRelName, currentNamespace);
                }
                catch (Exception e) {
                    LOG.error("Failed to resolve UnresolvedRelation: {}", uRelName, e);
                    return p;
                }
                String viewName = vastView.name();
                String query = vastView.query();
                LogicalPlan parsedQueryPlan;
                try {
                    parsedQueryPlan = session.sql(query).logicalPlan();
                }
                catch (Exception e) {
                    throw new RuntimeException(QueryCompilationErrors.invalidViewText(query, viewName));
                }
                Builder<String, List<String>> namespaceSeqBuilder = List.newBuilder();
                for (String part : vastView.currentNamespace()) {
                    namespaceSeqBuilder.addOne(part);
                }
                List<String> namespaceSeq = namespaceSeqBuilder.result();
                AliasIdentifier aliasIdentifier = AliasIdentifier.apply(viewName, namespaceSeq);
                LOG.debug("Resolved view plan with alias identifier {}: {}", aliasIdentifier, parsedQueryPlan);

                LogicalPlan newPlan = parsedQueryPlan;
                String[] columnAliases = vastView.columnAliases();
                if (columnAliases != null && columnAliases.length > 0) {
                    Seq<Attribute> output = parsedQueryPlan.output();
                    if (output != null && output.size() == columnAliases.length) {
                        Builder<NamedExpression, List<NamedExpression>> namedExpressionListBuilder = List$.MODULE$.newBuilder();
                        for (int i = 0; i < columnAliases.length; i++) {
                            String columnAlias = columnAliases[i];
                            addAlias(output.apply(i), columnAlias, namedExpressionListBuilder);
                        }
                        Seq<NamedExpression> projectList = namedExpressionListBuilder.result();
                        newPlan = new Project(projectList, parsedQueryPlan);
                    }
                    else {
                        throw new RuntimeException(format("Number of Aliases doesn't match number of projections. Aliases: %s, projections: %s",
                                Arrays.toString(columnAliases), newPlan.output()));
                    }
                }
                SubqueryAlias subqueryAlias = new SubqueryAlias(aliasIdentifier, newPlan);
                LOG.debug("Returning resolved view plan with subquery alias {}", subqueryAlias);
                return subqueryAlias;
            }
            else if (p instanceof UnresolvedTableOrView) {
                LOG.debug("Trying to resolve UnresolvedTableOrView: {}", p);
                Seq<String> uRelName = unresolvedIdentifierSeqSupplier.apply(p).get();
                String[] currentNamespace = session.sessionState().catalogManager().currentNamespace();
                VastView vastView;
                try {
                    vastView = this.viewLoader.apply(uRelName, currentNamespace);
                }
                catch (Exception e) {
                    LOG.error("Failed to resolve UnresolvedTableOrView: {}", uRelName, e);
                    return p;
                }
                Identifier identifier = Identifier.of(vastView.currentNamespace(), vastView.name());
                UnresolvedTableOrView unresolvedTableOrView = (UnresolvedTableOrView) p;
                boolean alterView = unresolvedTableOrView.commandName().startsWith("ALTER VIEW");
                if (alterView) {
                    StructType structType = session.sql(vastView.query()).logicalPlan().schema();
                    ResolvedPersistentView resolvedView = new ResolvedPersistentView(InitializedVastCatalog.getVastCatalog(), identifier, structType);
                    LOG.debug("Successfully transformed UnresolvedTableOrView to ResolvedPersistentView: {}", resolvedView);
                    return resolvedView;
                }
                else {
                    Table vastViewTable = vastView.asTable();
                    Builder<Attribute, List<Attribute>> attributeListBuilder = List.newBuilder();
                    for (StructField field : vastViewTable.schema().fields()) {
                        attributeListBuilder.addOne(field.toAttribute());
                    }
                    ResolvedTable resolvedTable = new ResolvedTable(InitializedVastCatalog.getVastCatalog(), identifier, vastViewTable, attributeListBuilder.result());
                    LOG.debug("Successfully transformed UnresolvedTableOrView to ResolvedTable: {}", resolvedTable);
                    return resolvedTable;
                }
            }
            else if (p instanceof UnresolvedView) {
                LOG.debug("Trying to resolve UnresolvedView: {}", p);
                Seq<String> uRelName = unresolvedIdentifierSeqSupplier.apply(p).get();
                String[] currentNamespace = session.sessionState().catalogManager().currentNamespace();
                VastView vastView;
                try {
                    vastView = this.viewLoader.apply(uRelName, currentNamespace);
                }
                catch (Exception e) {
                    LOG.error("Failed to resolve UnresolvedView: {}", uRelName, e);
                    return p;
                }
                LOG.debug("Loaded view from UnresolvedView: {}", vastView);
                Identifier identifier = Identifier.of(vastView.currentNamespace(), vastView.name());
                StructType structType = session.sql(vastView.query()).logicalPlan().schema();
                ResolvedPersistentView resolvedView = new ResolvedPersistentView(InitializedVastCatalog.getVastCatalog(), identifier, structType);
                LOG.debug("Successfully transformed UnresolvedView to ResolvedPersistentView: {}", resolvedView);
                return resolvedView;
            }
            else if (p instanceof CreateNDBViewPlan) {
                CreateNDBViewPlan createNDBViewPlan = (CreateNDBViewPlan) p;
                LogicalPlan resolvedQuery = session.sessionState().analyzer().execute(createNDBViewPlan.children().apply(1));
                LOG.debug("Successfully resolved CreateNDBViewPlan query to LogicalPlan: {}", resolvedQuery);
                LogicalPlan[] r = new LogicalPlan[] {createNDBViewPlan.children().apply(0), resolvedQuery};
                ArraySeq<LogicalPlan> result = ArraySeq$.MODULE$.unsafeWrapArray(r);
                return createNDBViewPlan.withNewChildrenInternal(result);
            }
            else {
                return p;
            }
        };
        PartialFunction<LogicalPlan, LogicalPlan> transformer = PartialFunction.fromFunction(resolveViewFunc);
        return plan.resolveOperators(transformer);
    }

    private static void addAlias(Expression outputField, String columnAlias, Builder<NamedExpression, List<NamedExpression>> namedExpressionListBuilder)
    {
        NamedExpression al = new Alias(outputField, columnAlias, NamedExpression.newExprId(), EMPTY_STRING_SEQ, Option.apply(Metadata.empty()), EMPTY_STRING_SEQ);
        namedExpressionListBuilder.addOne(al);
    }



    private synchronized VastCatalog getVastCatalog() {
        if (vastCatalog == null) {
            setVastCatalog();
        }
        return vastCatalog;
    }

    private synchronized void setVastCatalog()
    {
        if (vastCatalog == null) {
            vastCatalog = InitializedVastCatalog.getVastCatalog();
        }
    }
}
