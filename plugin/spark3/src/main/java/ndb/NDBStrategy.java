/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.spark.ExpressionsUtil;
import com.vastdata.spark.VastBatch;
import com.vastdata.spark.VastScan;
import com.vastdata.spark.VastScanBuilder;
import com.vastdata.spark.VastTable;
import com.vastdata.spark.statistics.AnalyzeNDBColumnCommand;
import com.vastdata.spark.statistics.AnalyzeNDBTableCommand;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.AttributeSet;
import org.apache.spark.sql.catalyst.expressions.DynamicPruning;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.AnalyzeColumn;
import org.apache.spark.sql.catalyst.plans.logical.AnalyzeTable;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData;
import org.apache.spark.sql.catalyst.plans.logical.ShowColumns;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.execution.FilterExec;
import org.apache.spark.sql.execution.ProjectExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.SparkStrategy;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.execution.datasources.v2.PushDownUtils;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.immutable.List;
import scala.collection.immutable.Seq;
import scala.collection.mutable.Builder;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.SecureRandom;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.vastdata.spark.VastDelete.DELETE_ERROR_SUPPLIER;
import static java.lang.String.format;

public class NDBStrategy extends SparkStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger(NDBStrategy.class);
    public static final List<SparkPlan> EMPTY_RESULT_SEQ = List.<SparkPlan>newBuilder().result();
    private static final SecureRandom secureRandom = new SecureRandom();

    @Override
    public scala.collection.immutable.Seq<SparkPlan> apply(LogicalPlan plan)
    {
        if (plan instanceof ShowColumns) {
            ShowNDBTableColumnsCommand ndbPlan = ShowNDBTableColumnsCommand.instance((ShowColumns) plan);
            LOG.debug("Returning {} for ShowColumns plan: {}", ndbPlan, plan);
            return planToResultImmutableSeq(ndbPlan);
        } else if (plan instanceof AnalyzeTable) {
            AnalyzeNDBTableCommand ndbPlan = AnalyzeNDBTableCommand.instance((AnalyzeTable) plan);
            LOG.debug("Returning {} for AnalyzeTable plan: {}", ndbPlan, plan);
            return planToResultImmutableSeq(ndbPlan);
        } else if (plan instanceof AnalyzeColumn) {
            AnalyzeNDBColumnCommand ndbPlan = AnalyzeNDBColumnCommand.instance((AnalyzeColumn) plan);
            LOG.debug("Returning {} for AnalyzeColumn plan: {}", ndbPlan, plan);
            return planToResultImmutableSeq(ndbPlan);
        }
        else if (plan instanceof Project) {
            return optimize(plan, secureRandom.nextInt(), (p, token) -> processProject((Project) p, token));
        }
        else if (plan instanceof Filter) {
            return optimize(plan, secureRandom.nextInt(), (p, token) -> processFilter((Filter) p, token));
        }
        else if (plan instanceof ReplaceData) { // when there is a filter that is not supported for pushdown, cos(x) for example
            ReplaceData replaceData = (ReplaceData) plan;
            LogicalPlan child = replaceData.child();
            if (child instanceof Filter) {
                Filter filter = (Filter) child;
                throw DELETE_ERROR_SUPPLIER.apply(filter.condition());
            }
        }
        LOG.debug("{} not supported by NDBStrategy", plan.getClass().getCanonicalName());
        return EMPTY_RESULT_SEQ;
    }

    private Seq<SparkPlan> optimize(LogicalPlan plan, int token, BiFunction<LogicalPlan, String, Seq<SparkPlan>> planOptimizer)
    {
        int planHashCode = plan.hashCode();
        String className = plan.getClass().getSimpleName();
        String logPrefix = format("%s:%s:%s", className, planHashCode, token);
        if (LOG.isDebugEnabled()) {
            LOG.debug("{} optimize details: \n{}", logPrefix, plan);
        }
        Seq<SparkPlan> result = planOptimizer.apply(plan, logPrefix);
        if (LOG.isDebugEnabled()) {
            LOG.debug("{} optimize result details: {}\n{}", logPrefix, getResultHashCode(result), result);
        }
        return result;
    }

    private static int getResultHashCode(Seq<SparkPlan> result)
    {
        return (result != null && result.size() > 0) ? result.apply(0).hashCode() : 0;
    }

    @Nullable
    private Seq<SparkPlan> processFilter(Filter filter, String traceToken)
    {
        LogicalPlan child = filter.child();
        if (Objects.equals(child.getClass().getCanonicalName(), DataSourceV2ScanRelation.class.getCanonicalName())) {
            Class<? extends LogicalPlan> v2RelClass = child.getClass();
            Scan scan;
            Seq<AttributeReference> relOutput;
            try {
                Method scanMethod = v2RelClass.getMethod("scan");
                Method relOutputMethod = v2RelClass.getMethod("output");
                scan = (Scan) scanMethod.invoke(child);
                relOutput = (Seq<AttributeReference>) relOutputMethod.invoke(child);
            }
            catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            if (scan instanceof VastScan) {
                VastTable vastTable = ((VastScan) scan).toBatch().getTable();
                Builder<Expression, List<Expression>> runtimeFiltersBuilder = List.newBuilder();
                Builder<Expression, List<Expression>> postFiltersBuilder = List.newBuilder();
                splitFilters(filter, runtimeFiltersBuilder, postFiltersBuilder);
                List<Expression> runtimeFilters = runtimeFiltersBuilder.result();
                BatchScanExec scanExec = newBatchScanExec(relOutput, scan, runtimeFilters, vastTable);

                Option<Expression> actualRequiredPostFilter = pushPostfilter(scan, postFiltersBuilder.result());
                if (actualRequiredPostFilter.isEmpty()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} Returning new BatchScanExec: {}", traceToken, scanExec.hashCode());
                    }
                    return planToResultImmutableSeq(scanExec);
                }
                else {
                    FilterExec filterExec = new FilterExec(actualRequiredPostFilter.get(), scanExec);
                    Seq<Attribute> output = filterExec.output();
                    Seq<AttributeReference> scanOutput = scanExec.output();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} Returning new Filter: {}, BatchScanExec: {}, with new filter output: {}, scanOutput: {}", traceToken, filterExec, scanExec.hashCode(), output, scanOutput);
                    }
                    return planToResultImmutableSeq(filterExec);
                }
            }
            else {
                LOG.warn("{} Skipping Filter-Scan - unrecognized scan: {}, of class: {}", traceToken, scan.hashCode(), scan.getClass());
                return EMPTY_RESULT_SEQ;
            }
        }
        else if (child instanceof Project) {
            LOG.debug("{} optimize for filter over project node", traceToken);
            Seq<SparkPlan> sparkPlanSeq = processProject((Project) child, traceToken);
            if (sparkPlanSeq == null || sparkPlanSeq.isEmpty()) {
                LOG.debug("{} optimize for filter over project node - skipping", traceToken);
                return EMPTY_RESULT_SEQ;
            }
            else {
                return reoptimizeForFilter(traceToken, filter, sparkPlanSeq, "project");
            }
        }
        else if (child instanceof Filter) {
            LOG.debug("{} optimize for filter over filter node", traceToken);
            Seq<SparkPlan> sparkPlanSeq = processFilter((Filter) child, traceToken);
            if (sparkPlanSeq == null || sparkPlanSeq.isEmpty()) {
                LOG.debug("{} optimize for filter over filter node - skipping", traceToken);
                return EMPTY_RESULT_SEQ;
            }
            else {
                return reoptimizeForFilter(traceToken, filter, sparkPlanSeq, "filter");
            }
        }
        else {
            LOG.warn("{} Skipping Filter child: {} of class: {}", traceToken, child.hashCode(), child.getClass());
            return EMPTY_RESULT_SEQ;
        }
    }

    private Seq<SparkPlan> reoptimizeForFilter(String traceToken, Filter filter, Seq<SparkPlan> planSeq, String origNodeName)
    {
        SparkPlan result = planSeq.apply(0);
        if (result instanceof BatchScanExec) {
            LOG.info("{} filter-{} Trying reoptimize {} over resulted BatchScanExec: {}", traceToken, origNodeName, filter.condition(), result);
            BatchScanExec scanExec = (BatchScanExec) result;
            Builder<Expression, List<Expression>> runtimeFiltersBuilder = List.newBuilder();
            Builder<Expression, List<Expression>> postFiltersBuilder = List.newBuilder();
            splitFilters(filter, runtimeFiltersBuilder, postFiltersBuilder);
            List<Expression> runtimeFilters = runtimeFiltersBuilder.result();
            List<Expression> postFilters = postFiltersBuilder.result();

            VastScan origScan = (VastScan) scanExec.scan();
            VastScanBuilder builder = VastScanBuilder.rebuildScan(origScan);

            Option<Expression> actualRequiredPostFilter;
            try {
                actualRequiredPostFilter = PushDownUtils.pushFilters(builder, postFilters)._2.reduceLeftOption(And::new);
            }
            catch (RuntimeException re) {
                LOG.error(format("Filter pushdown failed for filters: %s", postFilters), re);
                throw re;
            }
            Builder<AttributeReference, List<AttributeReference>> outputBuilder = List.newBuilder();
            result.output().foreach(a -> outputBuilder.$plus$eq((AttributeReference) a));

            BatchScanExec newBatchScanExec = newBatchScanExec(outputBuilder.result(), builder.build(), runtimeFilters, scanExec.table());
            if (actualRequiredPostFilter.isEmpty()) {
                LOG.info("{} Filter-{} reoptimize returning new BatchScanExec", traceToken, origNodeName);
                return planToResultImmutableSeq(newBatchScanExec);
            }
            else {
                FilterExec newFilter = new FilterExec(actualRequiredPostFilter.get(), newBatchScanExec);
                LOG.info("{} Filter-{} reoptimize returning new Filter", traceToken, origNodeName);
                return planToResultImmutableSeq(newFilter);
            }
        }
        else {
            LOG.info("{} optimize for filter over {} node - return new filter", traceToken, origNodeName);
            FilterExec newFilter = new FilterExec(filter.condition(), result);
            return planToResultImmutableSeq(newFilter);
        }
    }

    private static void splitFilters(Filter filter, Builder<Expression, List<Expression>> runtimeFiltersBuilder, Builder<Expression, List<Expression>> postFiltersBuilder)
    {
        scala.collection.Seq<Expression> filters = ExpressionsUtil.split(filter.condition());
        filters.foreach(exp -> {
            if (exp instanceof DynamicPruning) {
                runtimeFiltersBuilder.$plus$eq(exp);
            }
            else {
                postFiltersBuilder.$plus$eq(exp);
            }
            return null;
        });
    }

    @NotNull
    private static BatchScanExec newBatchScanExec(Seq<AttributeReference> output, Scan scan, Seq<Expression> runtimeFilters, Table table)
    {
        return new BatchScanExec(output, scan, runtimeFilters, Option.empty(), Option.empty(), table, Option.empty(), false, false);
    }

    private static Option<Expression> pushPostfilter(Scan scan, List<Expression> postFilters)
    {
        ScanBuilder builder = VastScanBuilder.rebuildScan((VastScan) scan);
        try {
            return PushDownUtils.pushFilters(builder, postFilters)._2.reduceLeftOption(And::new);
        }
        catch (RuntimeException re) {
            LOG.error(format("Filter pushdown failed for filters: %s", postFilters), re);
            throw re;
        }
    }

    @Nullable
    private Seq<SparkPlan> processProject(Project project, String traceToken)
    {
        LogicalPlan child = project.child();
        if (child instanceof Filter) {
            Filter filter = (Filter) child;
            LogicalPlan filterChild = filter.child();
            // instanceof might fail because of class loader overriding
            if (Objects.equals(filterChild.getClass().getCanonicalName(), DataSourceV2ScanRelation.class.getCanonicalName())) {
                Class<? extends LogicalPlan> v2RelClass = filterChild.getClass();
                Scan scan;
                Seq<AttributeReference> currentScanOutput;
                try {
                    Method scanMethod = v2RelClass.getMethod("scan");
                    Method relOutputMethod = v2RelClass.getMethod("output");
                    scan = (Scan) scanMethod.invoke(filterChild);
                    currentScanOutput = (Seq<AttributeReference>) relOutputMethod.invoke(filterChild);
                }
                catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
                if (scan instanceof VastScan) {
                    Builder<Expression, List<Expression>> runtimeFiltersBuilder = List.newBuilder();
                    Builder<Expression, List<Expression>> postFiltersBuilder = List.newBuilder();
                    splitFilters(filter, runtimeFiltersBuilder, postFiltersBuilder);
                    List<Expression> runtimeFilters = runtimeFiltersBuilder.result();
                    Option<Expression> actualRequiredPostFilter = pushPostfilter(scan, postFiltersBuilder.result());
                    StructType currentScanSchema = scan.readSchema();
                    Seq<NamedExpression> projectedColumns = project.projectList();
                    Set<StructField> requiredProjectSchema = new LinkedHashSet<>();
                    Builder<AttributeReference, List<AttributeReference>> newScanOutputBuilder = List.newBuilder();
                    Function<Attribute, Attribute> schemaAttributeAdder = attrRef -> {
                        String name = attrRef.name();
                        Option<Object> fieldIndex = currentScanSchema.getFieldIndex(name);
                        if (!fieldIndex.isEmpty()) {
                            if (requiredProjectSchema.add(currentScanSchema.apply(fieldIndex.get()))) {
                                LOG.debug("Optimized scan schema: adding field: {}", name);
                                newScanOutputBuilder.addOne((AttributeReference) attrRef);
                            }
                            else {
                                LOG.debug("Optimized scan schema: skipping already existing field: {}", name);
                            }
                        }
                        else {
                            LOG.warn("Optimized scan schema: field not found: {}, exp: {}, of class: {}", name, attrRef, attrRef.getClass());
                        }
                        return null;
                    };
                    AtomicBoolean needProjectNode = new AtomicBoolean(false);
                    projectedColumns.foreach(exp -> {
                        if (!(exp instanceof Attribute)) {
                            needProjectNode.set(true);
                        }
                        AttributeSet references = ((Expression) exp).references(); // without explicit casting compilation fails
                        references.foreach(schemaAttributeAdder::apply);
                        return null;
                    });
                    if (actualRequiredPostFilter.isEmpty()) {
                        VastTable vastTable = ((VastScan) scan).toBatch().getTable();
                        BatchScanExec newScanExec = adaptScanIfNeeded(vastTable, scan, currentScanOutput, currentScanSchema, requiredProjectSchema,
                                newScanOutputBuilder, runtimeFilters, traceToken);
                        if (!needProjectNode.get()) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("{} Returning new scan, BatchScanExec: {}", traceToken, newScanExec.hashCode());
                            }
                            return planToResultImmutableSeq(newScanExec);
                        }
                        else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("{} wrapping scan with project node, BatchScanExec: {}", traceToken, newScanExec.hashCode());
                            }
                            return planToResultImmutableSeq(newProjectNode(projectedColumns, newScanExec));
                        }
                    }
                    else {
                        Expression postFilter = actualRequiredPostFilter.get();
                        AttributeSet postFilterAttributes = postFilter.references();
                        LOG.info("{} Required post-filtering: {}, with attribute set: {}", traceToken, postFilter, postFilterAttributes);
                        postFilterAttributes.foreach(schemaAttributeAdder::apply);
                        VastTable vastTable = ((VastScan) scan).toBatch().getTable();
                        BatchScanExec newScanExec = adaptScanIfNeeded(vastTable, scan, currentScanOutput, currentScanSchema, requiredProjectSchema,
                                newScanOutputBuilder, runtimeFilters, traceToken);
                        FilterExec filterExec = new FilterExec(actualRequiredPostFilter.get(), newScanExec);
                        ProjectExec projectExec = newProjectNode(projectedColumns, filterExec);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("{} Returning new Project-Filter-Scan node, BatchScanExec: {}, Filter: {}, Project: {}",
                                    traceToken, newScanExec.hashCode(), filterExec.hashCode(), projectExec.hashCode());
                        }
                        return planToResultImmutableSeq(projectExec);
                    }
                }
                else {
                    LOG.warn("{} Skipping Project-Filter-Scan - unrecognized scan {} of class {}", traceToken, scan.hashCode(), scan.getClass());
                    return EMPTY_RESULT_SEQ;
                }
            }
            else {
                LOG.warn("{} Skipping Project-Filter child - unrecognized filter child: {}, of class: {}", traceToken, filterChild.hashCode(), filterChild.getClass());
                return EMPTY_RESULT_SEQ;
            }
        }
        else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("{} Skipping Project child: {}, of class: {}", traceToken, child.hashCode(), child.getClass());
            }
            return EMPTY_RESULT_SEQ;
        }
    }

    @NotNull
    private static ProjectExec newProjectNode(Seq<NamedExpression> projectedColumns, SparkPlan childPlan)
    {
        return new ProjectExec(getProjectOutput(projectedColumns), childPlan);
    }

    private static List<NamedExpression> getProjectOutput(Seq<NamedExpression> projectedColumns)
    {
        Builder<NamedExpression, List<NamedExpression>> projectedNamesExpressions = List.newBuilder();
        projectedColumns.foreach(projectedNamesExpressions::$plus$eq);
        return projectedNamesExpressions.result();
    }

    private BatchScanExec adaptScanIfNeeded(VastTable table, Scan scan, Seq<AttributeReference> origOutput, StructType currentScanSchema, Set<StructField> requiredSchemaFields,
            Builder<AttributeReference, List<AttributeReference>> newOutputBuilder, Seq<Expression> runtimeFilters, String logPrefix)
    {
        if (currentScanSchema.size() == requiredSchemaFields.size()) {
            LOG.debug("{} returning new BatchScanExec using same scan", logPrefix);
            return newBatchScanExec(origOutput, scan, runtimeFilters, table);
//            return new BatchScanExec(origOutput, scan, runtimeFilters, Option.empty(), table);
        }
        else {
            LOG.debug("{} returning new BatchScanExec using adapted scan", logPrefix);
            List<AttributeReference> newOutput = newOutputBuilder.result();
            ((VastBatch) scan.toBatch()).updateProjections(new StructType(requiredSchemaFields.toArray(new StructField[0])));
            return newBatchScanExec(newOutput, scan, runtimeFilters, table);
//            return new BatchScanExec(newOutput, scan, runtimeFilters, Option.empty(), table);
        }
    }

    private scala.collection.immutable.Seq<SparkPlan> planToResultImmutableSeq(SparkPlan plan)
    {
        Builder<SparkPlan, List<SparkPlan>> builder = List.newBuilder();
        builder.$plus$eq(plan);
        return builder.result();
    }
}
