/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.vastdata.client.error.ErrorType;
import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.spark.predicate.VastPredicate;
import com.vastdata.spark.predicate.VastPredicatePushdown;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.sql.catalog.ndb.TypeUtil;
import spark.sql.catalog.ndb.VastCatalogUtils;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import static com.vastdata.client.partition.PartitionConstants.PIT_NAME_SUFFIX;
import static com.vastdata.spark.ParsedRowColumnSecurity.getParsedRowColumnSecurity;
import static com.vastdata.spark.SparkArrowVectorUtil.VASTDB_SPARK_DEC128_ROW_ID_NONNULL;
import static com.vastdata.spark.SparkArrowVectorUtil.VASTDB_SPARK_INT64_ROW_ID_NONNULL;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static spark.sql.catalog.ndb.TypeUtil.schemaHasCharNType;

public class VastScanBuilder
        implements SupportsPushDownV2Filters, SupportsPushDownRequiredColumns,
        SupportsPushDownLimit, SupportsPushDownAggregates
{
    private static final Logger LOG = LoggerFactory.getLogger(
            VastScanBuilder.class);
    private static final SecureRandom scanBuilderIdProvider = new SecureRandom();

    protected final VastTable table;
    protected final List<Expression> postFilter;
    protected final Map<String, Expression> columnMasks;
    protected final int scanBuilderID = scanBuilderIdProvider.nextInt();
    private final Map<String, Expression> relevantMaskExpressions = new HashMap<>();
    protected List<List<VastPredicate>> pushedDownPredicates;
    protected StructType schema;
    protected Integer limit;
    private boolean enablePredicatePushdown = true;

    public VastScanBuilder(VastTable table, VastCatalogUtils vastCatalogUtils)
    {
        this.table = table;
        this.schema = table.schema();
        if (this.schema.isEmpty()) {
            throw new VastRuntimeException("Schema is empty", null,
                    ErrorType.USER);
        }
        this.limit = null;
        ParsedRowColumnSecurity rowColumnSecurityPushdowns = getRowColumnSecurityPushdowns(
                vastCatalogUtils);
        vastCatalogUtils.checkScanIsAllowed(
                        table.getTableMD().tableName,
                        !rowColumnSecurityPushdowns.getPostFilter().isEmpty(),
                        !rowColumnSecurityPushdowns.getPushedPredicates().isEmpty()
                );

        this.pushedDownPredicates = rowColumnSecurityPushdowns.getPushedPredicates();
        this.postFilter = new ArrayList<>(
                rowColumnSecurityPushdowns.getPostFilter());
        this.columnMasks = rowColumnSecurityPushdowns.getColumnMasks();
        LOG.debug("new VastScanBuilder({}, {}), scanBuilderID={}", table.name(),
                schema, scanBuilderID);
    }

    private ParsedRowColumnSecurity getRowColumnSecurityPushdowns(
            VastCatalogUtils vastCatalogUtils)
    {
        return getParsedRowColumnSecurity(vastCatalogUtils,
                table.getSchemaName(), table.getTableMD().tableName, schema,
                false);
    }

    public void disablePredicatePushdown()
    {
        this.enablePredicatePushdown = false;
    }

    @Override
    public Scan build()
    {
        LOG.debug("{}:{} build new VastScan with predicates {}", table.name(),
                scanBuilderID, pushedDownPredicates);
        return new VastScan(scanBuilderID, table, schema, limit,
                pushedDownPredicates);
    }

    @Override
    public void pruneColumns(StructType requiredSchema)
    {
        LOG.info("{}:{} pruneColumns: {}", table.name(), scanBuilderID,
                requiredSchema);
        HashSet<String> columnsForPostfilter = new HashSet<>();
        postFilter.forEach(
                postFilterNode -> postFilterNode.references().foreach(ref -> {
                    columnsForPostfilter.add(ref.name());
                    return null;
                }));
        requiredSchema.foreach(f -> {
            columnsForPostfilter.remove(f.name());
            return null;
        });
        for (Map.Entry<String, Expression> columnsMaskEntry : columnMasks.entrySet()) {
            String columnName = columnsMaskEntry.getKey();
            if (requiredSchema.getFieldIndex(columnName).nonEmpty()) {
                relevantMaskExpressions.put(columnsMaskEntry.getKey(),
                        columnsMaskEntry.getValue());
            }
        }
        for (String column : columnsForPostfilter) {
            StructField field = this.schema.apply(column);
            requiredSchema = requiredSchema.add(field);
        }
        LOG.info("{}:{} pruneColumns after postfilter: {}", table.name(),
                scanBuilderID, requiredSchema);
        // Adaptation is needed because spark uses string type for char(n) columns - get original column type
        this.schema = !this.schema.existsRecursively(schemaHasCharNType) ?
                requiredSchema :
                adaptRequiredSchemaToTableSchema(this.schema, requiredSchema);
    }

    private StructType adaptRequiredSchemaToTableSchema(StructType currSchema,
            StructType requiredSchema)
    {
        List<Field> currFields = TypeUtil.sparkSchemaToArrowFieldsList(
                currSchema);
        Map<String, Field> currFieldsByNameMap = Maps.uniqueIndex(currFields,
                Field::getName);
        List<Field> collectedRequiredFields = TypeUtil
                .sparkSchemaToArrowFieldsList(requiredSchema)
                .stream()
                .map(f -> {
                    if (VASTDB_SPARK_INT64_ROW_ID_NONNULL.getName().equals(
                            f.getName())) {
                        return VASTDB_SPARK_INT64_ROW_ID_NONNULL;
                    }
                    if (VASTDB_SPARK_DEC128_ROW_ID_NONNULL.getName().equals(
                            f.getName())) {
                        return VASTDB_SPARK_DEC128_ROW_ID_NONNULL;
                    }
                    else {
                        Field field = currFieldsByNameMap.get(f.getName());
                        if (field == null) {
                            throw new IllegalStateException(
                                    format("Unexpected require schema field: %s",
                                            f));
                        }
                        return field;
                    }

                })
                .collect(Collectors.toList());
        return TypeUtil.arrowFieldsListToSparkSchema(collectedRequiredFields);
    }

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates)
    {
        if (!enablePredicatePushdown) {
            LOG.info(
                    "{}:{} PREDICATES pushdown is disabled: {} will be post-filtered",
                    table.name(), scanBuilderID, predicates);
            return predicates;
        }
        LOG.info("{}:{} PREDICATES pushdown: current={}, new={}", table.name(),
                scanBuilderID, pushedDownPredicates, predicates);
        VastPredicatePushdown result = VastPredicatePushdown.parse(predicates,
                schema);
        ImmutableList.Builder<List<VastPredicate>> allPredicates = ImmutableList.builder();

        allPredicates.addAll(pushedDownPredicates).addAll(
                result.getPushedDown());
        pushedDownPredicates = allPredicates.build();
        return result.getPostFilter().toArray(new Predicate[0]);
    }

    @Override
    public Predicate[] pushedPredicates()
    {
        BinaryOperator<Predicate> oriBinPred = (p1, p2) -> new Predicate("OR",
                new Predicate[] {p1, p2});
        LOG.debug("pushedPredicates: {}", pushedDownPredicates);
        return this.pushedDownPredicates
                .stream()
                .map(predList -> predList
                        .stream()
                        .map(VastPredicate::getPredicate)
                        .reduce(oriBinPred)
                        .orElseThrow(() -> new VerifyException(
                                "Predicate not preset")))
                .toArray(Predicate[]::new);
    }

    @Override
    public boolean pushLimit(int limit)
    {
        LOG.info("{}:{} LIMIT pushdown: current={}, limit={}", table.name(),
                scanBuilderID, this.limit, limit);
        this.limit = isNull(this.limit) ? limit : Math.min(this.limit, limit);
        return false; // Since a single split can enforce only its own "local" limit, we need Spark to apply a "global" limit.
        // For example, if we pushdown `LIMIT 1`, each split will return a single row, and Spark should return exactly one of them to the user.
    }

    @Override
    public boolean supportCompletePushDown(Aggregation aggregation)
    {
        LOG.info("{}:{} supportCompletePushDown: {}", table.name(),
                scanBuilderID, describeAggregation(aggregation));
        return SupportsPushDownAggregates.super.supportCompletePushDown(
                aggregation);
    }

    @Override
    public boolean pushAggregation(Aggregation aggregation)
    {
        LOG.info("{}:{} pushAggregation: {}", table.name(), scanBuilderID,
                describeAggregation(aggregation));
        return false;
    }

    private String describeAggregation(Aggregation aggregation)
    {
        return format("func:%s, group_by:%s",
                Arrays.toString(aggregation.aggregateExpressions()),
                Arrays.toString(aggregation.groupByExpressions()));
    }

    public Map<String, Expression> getColumnMasks()
    {
        return columnMasks;
    }

    public List<Expression> getPostFilter()
    {
        return postFilter;
    }
}
