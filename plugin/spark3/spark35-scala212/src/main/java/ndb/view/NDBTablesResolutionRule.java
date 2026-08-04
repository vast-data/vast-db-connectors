/*
 *  Copyright (C) Vast Data Ltd.
 */
package ndb.view;

import com.vastdata.client.ParsedURL;
import com.vastdata.client.error.ErrorType;
import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.spark.ParsedRowColumnSecurity;
import com.vastdata.spark.VastScanBuilder;
import com.vastdata.spark.VastTable;
import com.vastdata.spark.VastView;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.AliasIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.ResolvedPersistentView;
import org.apache.spark.sql.catalyst.analysis.ResolvedTable;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.analysis.UnresolvedTableOrView;
import org.apache.spark.sql.catalyst.analysis.UnresolvedView;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.catalyst.trees.TreeNodeTag;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.errors.QueryCompilationErrors;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.PartialFunction;
import scala.PartialFunction$;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.IndexedSeq$;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.mutable.Builder;
import spark.sql.catalog.ndb.InitializedVastCatalog;
import spark.sql.catalog.ndb.VastCatalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.vastdata.spark.ParsedRowColumnSecurity.getParsedRowColumnSecurity;
import static java.lang.String.format;
import static ndb.SparkPlannerUtil.nameSeqWithoutRCLSSuffix;
import static ndb.SparkPlannerUtil.newAlias;
import static ndb.SparkPlannerUtil.newDataSourceV2Relation;
import static ndb.SparkPlannerUtil.removeRowLevelOpSuffixFromNameSeq;
import static ndb.SparkPlannerUtil.removeVastResolutionSuffixes;
import static spark.sql.catalog.ndb.NDBRowLevelOperationIdentifier.isForRowLevelOp;

public class NDBTablesResolutionRule
        extends Rule<LogicalPlan>
{
    public static final String VAST_THROW_RCLS_ERROR = "_vast_throw_rcls_error";
    private static final TreeNodeTag<? super Boolean> VAST_TAGS_FOR_DELETE_FROM_TABLE = new TreeNodeTag<>(
            "_VAST_TAGS_FOR_DELETE_FROM_TABLE");
    private static final Logger LOG = LoggerFactory.getLogger(
            NDBTablesResolutionRule.class);
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
        else {
            throw new RuntimeException(
                    "Unexpected class for unresolved identifier resolution: " + p.getClass());
        }
    };
    private final SparkSession session;
    private VastCatalog vastCatalog = null;
    final BiFunction<Seq<String>, String[], VastView> viewLoader = (uRelName, currentNamespace) -> {
        String[] namespaceForLookup;
        try {
            namespaceForLookup = new VastNamespaceResolver().apply(uRelName,
                    currentNamespace);
        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Failed to resolve namespace for view: " + uRelName, e);
        }
        Identifier ident = Identifier.of(namespaceForLookup, uRelName.last());
        LOG.debug("Trying to load view for identifier {}", ident);
        try {
            VastView vastView = getVastCatalog().loadView(ident,
                    Optional.empty());
            LOG.debug("Loaded view: {}", vastView);
            return vastView;
        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Unable to load view for identifier: " + ident, e);
        }
    };

    public NDBTablesResolutionRule(SparkSession session)
    {
        this.session = session;
    }

    private static LogicalPlan wrapLogicalPlanWithRCLSNodes(LogicalPlan plan,
            java.util.List<Expression> postFilter,
            Map<String, Expression> columnMaskExpressions)
    {
        LogicalPlan resultPlan = plan;
        Expression newFilterExpression;
        if (postFilter != null && !postFilter.isEmpty()) {
            Iterator<Expression> it = postFilter.iterator();
            newFilterExpression = it.next();
            while (it.hasNext()) {
                newFilterExpression = new And(newFilterExpression, it.next());
            }
            resultPlan = new Filter(newFilterExpression, resultPlan);
        }
        if (columnMaskExpressions != null && !columnMaskExpressions.isEmpty()) {
            Seq<Attribute> currentProjections = plan.output();
            java.util.List<NamedExpression> collectedList = JavaConverters
                    .asJavaCollection(currentProjections)
                    .stream()
                    .map(attribute -> {
                        if (columnMaskExpressions.containsKey(
                                attribute.name())) {
                            return (NamedExpression) newAlias(
                                    columnMaskExpressions.get(attribute.name()),
                                    attribute.name());
                        }
                        else {
                            return attribute;
                        }
                    })
                    .collect(Collectors.toList());
            Seq<NamedExpression> newProjections = JavaConverters.asScalaBuffer(
                    collectedList).toSeq();
            resultPlan = new Project(newProjections, resultPlan);
        }
        return resultPlan;
    }

    private static void addAlias(Expression outputField, String columnAlias,
            Builder<NamedExpression, List<NamedExpression>> namedExpressionListBuilder)
    {
        NamedExpression al = newAlias(outputField, columnAlias);
        namedExpressionListBuilder.$plus$eq(al);
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan)
    {
        if (plan instanceof DropNDBViewPlan || plan instanceof ShowNDBViewsPlan) {
            return plan;
        }
        Function1<LogicalPlan, LogicalPlan> unresolvedTablesResolver = p -> {
            if (p instanceof UnresolvedRelation) {
                LOG.debug("Trying to resolve UnresolvedRelation: {}", p);
                Seq<String> uRelName = unresolvedIdentifierSeqSupplier
                        .apply(p)
                        .get();
                String[] currentNamespace = session
                        .sessionState()
                        .catalogManager()
                        .currentNamespace();
                try {
                    Seq<String> nameForViewsLookup = isForRowLevelOp(
                            uRelName.last()) ?
                            removeRowLevelOpSuffixFromNameSeq(
                                    nameSeqWithoutRCLSSuffix(uRelName)) :
                            nameSeqWithoutRCLSSuffix(uRelName);
                    VastView vastView = this.viewLoader.apply(
                            nameForViewsLookup, currentNamespace);
                    if (p
                            .getTagValue(VAST_TAGS_FOR_DELETE_FROM_TABLE)
                            .isDefined()) {
                        throw new VastRuntimeException(
                                "Delete from a view is not supported", null,
                                ErrorType.USER);
                    }
                    String viewName = vastView.name();
                    String query = vastView.query();
                    LogicalPlan parsedQueryPlan;
                    try {
                        parsedQueryPlan = session.sql(query).logicalPlan();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(
                                QueryCompilationErrors.invalidViewText(query,
                                        viewName));
                    }
                    Builder<String, List<String>> namespaceSeqBuilder = List$.MODULE$.newBuilder();
                    for (String part : vastView.currentNamespace()) {
                        namespaceSeqBuilder.$plus$eq(part);
                    }
                    List<String> namespaceSeq = namespaceSeqBuilder.result();
                    AliasIdentifier aliasIdentifier = AliasIdentifier.apply(
                            viewName, namespaceSeq);
                    LOG.debug("Resolved view plan with alias identifier {}: {}",
                            aliasIdentifier, parsedQueryPlan);

                    LogicalPlan newPlan = parsedQueryPlan;
                    String[] columnAliases = vastView.columnAliases();
                    if (columnAliases != null && columnAliases.length > 0) {
                        Seq<Attribute> output = parsedQueryPlan.output();
                        if (output.size() == columnAliases.length) {
                            Builder<NamedExpression, List<NamedExpression>> namedExpressionListBuilder = List$.MODULE$.newBuilder();
                            for (int i = 0; i < columnAliases.length; i++) {
                                String columnAlias = columnAliases[i];
                                addAlias(output.apply(i), columnAlias,
                                        namedExpressionListBuilder);
                            }
                            Seq<NamedExpression> projectList = namedExpressionListBuilder.result();
                            newPlan = new Project(projectList, parsedQueryPlan);
                        }
                        else {
                            throw new RuntimeException(
                                    format("Number of Aliases doesn't match number of projections. Aliases: %s, projections: %s",
                                            Arrays.toString(columnAliases),
                                            newPlan.output()));
                        }
                    }
                    LogicalPlan newPlanWrappedWithRCLSNodes = wrappedWithRCLSNodes(
                            newPlan, vastView.currentNamespace(),
                            vastView.name());
                    SubqueryAlias subqueryAlias = new SubqueryAlias(
                            aliasIdentifier, newPlanWrappedWithRCLSNodes);
                    LOG.debug(
                            "Returning resolved view plan with subquery alias {}",
                            subqueryAlias);
                    return subqueryAlias;
                }
                catch (VastRuntimeException e) {
                    LOG.debug(
                            "Skipping RCLS resolution because of vast error: {}",
                            e.getMessage());
                    throw e;
                }
                catch (Exception e) {
                    LOG.error(
                            "Failed to resolve UnresolvedRelation as a view: {}",
                            uRelName, e);
                    try {
                        if (ifRCLSName(uRelName.last())) {
                            return resolveRCLSTableScanPlan(uRelName,
                                    currentNamespace);
                        }
                        else {
                            return p;
                        }
                    }
                    catch (Exception e2) {
                        LOG.error(
                                "Failed to resolve UnresolvedRelation as a RCLS-enabled table: {}",
                                uRelName, e2);
                        return removeVastResolutionSuffixes(
                                (UnresolvedRelation) p);
                    }
                }
            }
            else if (p instanceof UnresolvedTableOrView) {
                Seq<String> uRelName = unresolvedIdentifierSeqSupplier
                        .apply(p)
                        .get();
                LOG.debug(
                        "Trying to resolve UnresolvedTableOrView as a RCLS-enabled table: {}",
                        uRelName);
                try {
                    if (ifRCLSName(uRelName.last())) {
                        String[] currentNamespace = session
                                .sessionState()
                                .catalogManager()
                                .currentNamespace();
                        return resolveRCLSTableScanPlan(uRelName,
                                currentNamespace);
                    }
                }
                catch (NoSuchTableException e) {
                    LOG.debug(
                            "Trying to resolve UnresolvedTableOrView as a view: {}",
                            uRelName);
                }
                String[] currentNamespace = session
                        .sessionState()
                        .catalogManager()
                        .currentNamespace();
                VastView vastView;
                try {
                    vastView = this.viewLoader.apply(
                            nameSeqWithoutRCLSSuffix(uRelName),
                            currentNamespace);
                }
                catch (Exception e) {
                    LOG.error("Failed to resolve UnresolvedTableOrView: {}",
                            uRelName, e);
                    return p;
                }
                Identifier identifier = Identifier.of(
                        vastView.currentNamespace(), vastView.name());
                UnresolvedTableOrView unresolvedTableOrView = (UnresolvedTableOrView) p;
                boolean alterView = unresolvedTableOrView
                        .commandName()
                        .startsWith("ALTER VIEW");
                if (alterView) {
                    StructType structType = session
                            .sql(vastView.query())
                            .logicalPlan()
                            .schema();
                    ResolvedPersistentView resolvedView = new ResolvedPersistentView(
                            InitializedVastCatalog.getVastCatalog(), identifier,
                            structType);
                    LOG.debug(
                            "Successfully transformed UnresolvedTableOrView to ResolvedPersistentView: {}",
                            resolvedView);
                    return resolvedView;
                }
                else {
                    Table vastViewTable = vastView.asTable();
                    Builder<Attribute, List<Attribute>> attributeListBuilder = List$.MODULE$.newBuilder();
                    for (StructField field : vastViewTable.schema().fields()) {
                        attributeListBuilder.$plus$eq(
                                new AttributeReference(field.name(),
                                        field.dataType(), field.nullable(),
                                        field.metadata(),
                                        NamedExpression.newExprId(),
                                        List$.MODULE$.empty()));
                    }
                    ResolvedTable resolvedTable = new ResolvedTable(
                            InitializedVastCatalog.getVastCatalog(), identifier,
                            vastViewTable, attributeListBuilder.result());
                    LOG.debug(
                            "Successfully transformed UnresolvedTableOrView to ResolvedTable: {}",
                            resolvedTable);
                    return resolvedTable;
                }
            }
            else if (p instanceof UnresolvedView) {
                LOG.debug("Trying to resolve UnresolvedView: {}", p);
                Seq<String> uRelName = unresolvedIdentifierSeqSupplier
                        .apply(p)
                        .get();
                String[] currentNamespace = session
                        .sessionState()
                        .catalogManager()
                        .currentNamespace();
                VastView vastView;
                try {
                    vastView = this.viewLoader.apply(uRelName,
                            currentNamespace);
                }
                catch (Exception e) {
                    LOG.error("Failed to resolve UnresolvedView: {}", uRelName,
                            e);
                    return p;
                }
                LOG.debug("Loaded view from UnresolvedView: {}", vastView);
                Identifier identifier = Identifier.of(
                        vastView.currentNamespace(), vastView.name());
                StructType structType = session
                        .sql(vastView.query())
                        .logicalPlan()
                        .schema();
                ResolvedPersistentView resolvedView = new ResolvedPersistentView(
                        InitializedVastCatalog.getVastCatalog(), identifier,
                        structType);
                LOG.debug(
                        "Successfully transformed UnresolvedView to ResolvedPersistentView: {}",
                        resolvedView);
                return resolvedView;
            }
            else if (p instanceof CreateNDBViewPlan) {
                CreateNDBViewPlan createNDBViewPlan = (CreateNDBViewPlan) p;
                LogicalPlan resolvedQuery = session
                        .sessionState()
                        .analyzer()
                        .execute(createNDBViewPlan.children().apply(1));
                LOG.debug(
                        "Successfully resolved CreateNDBViewPlan query to LogicalPlan: {}",
                        resolvedQuery);
                LogicalPlan[] r = new LogicalPlan[] {createNDBViewPlan.children().apply(
                        0), resolvedQuery};
                Builder<LogicalPlan, IndexedSeq<LogicalPlan>> builder = IndexedSeq$.MODULE$.newBuilder();
                Arrays.stream(r).forEach(builder::$plus$eq);
                return createNDBViewPlan.withNewChildrenInternal(
                        builder.result());
            }
            else if (p instanceof DeleteFromTable) {
                DeleteFromTable deleteFromTable = (DeleteFromTable) p;
                LogicalPlan table = deleteFromTable.table();
                table.setTagValue(VAST_TAGS_FOR_DELETE_FROM_TABLE, true);
                return p;
            }
            else {
                return p;
            }
        };
        PartialFunction<LogicalPlan, LogicalPlan> transformer = PartialFunction$.MODULE$.apply(
                unresolvedTablesResolver);
        return plan.resolveOperators(transformer);
    }

    private LogicalPlan wrappedWithRCLSNodes(LogicalPlan plan,
            String[] schemaPathParts, String name)
    {
        StructType schema = plan.schema();
        String schemaPath = ParsedURL.compose(schemaPathParts);
        LOG.debug("DBG wrappedWithRCLSNodes schema={}, schemaPath={}", schema,
                schemaPath);
        ParsedRowColumnSecurity parsedRowColumnSecurity = getParsedRowColumnSecurity(
                InitializedVastCatalog.getVastCatalog().getVastCatalogUtils(),
                schemaPath, name, schema, false);

        ArrayList<Expression> postFilter = new ArrayList<>(
                parsedRowColumnSecurity.getPostFilter());
        Map<String, Expression> columnMasks = parsedRowColumnSecurity.getColumnMasks();
        return wrapLogicalPlanWithRCLSNodes(plan, postFilter, columnMasks);
    }

    private boolean ifRCLSName(String name)
    {
        return name.endsWith(VAST_THROW_RCLS_ERROR);
    }

    private LogicalPlan resolveRCLSTableScanPlan(Seq<String> uRelName,
            String[] currentNamespace)
            throws NoSuchTableException
    {
        String[] namespace = new VastNamespaceResolver().apply(uRelName,
                currentNamespace);
        String name = uRelName.apply(uRelName.size() - 1);
        String adaptedName = name.substring(0,
                name.length() - VAST_THROW_RCLS_ERROR.length());
        VastTable table = (VastTable) getVastCatalog().loadTable(
                Identifier.of(namespace, adaptedName));
        return wrapLogicalPlanWithRCLSNodes(table, namespace, name);
    }

    private LogicalPlan wrapLogicalPlanWithRCLSNodes(VastTable table,
            String[] namespace, String name)
    {
        VastScanBuilder scanBuilder = (VastScanBuilder) table.newScanBuilder(
                null);
        DataSourceV2Relation v2Relation = newDataSourceV2Relation(table,
                getVastCatalog(), namespace, name);
        java.util.List<Expression> postFilter = scanBuilder.getPostFilter();
        Map<String, Expression> columnMaskExpressions = scanBuilder.getColumnMasks();
        return wrapLogicalPlanWithRCLSNodes(v2Relation, postFilter,
                columnMaskExpressions);
    }

    private synchronized VastCatalog getVastCatalog()
    {
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
