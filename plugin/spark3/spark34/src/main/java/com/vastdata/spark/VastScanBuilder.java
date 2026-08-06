/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.vastdata.client.RowColumnSecurityResponse;
import com.vastdata.spark.predicate.VastPredicate;
import com.vastdata.spark.predicate.VastPredicatePushdown;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.util.V2ExpressionBuilder;
import org.apache.spark.sql.connector.expressions.FieldReference;
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
import scala.Option;
import spark.sql.catalog.ndb.TypeUtil;
import spark.sql.catalog.ndb.VastCatalogUtils;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static ndb.NDBSparkSessionExtension.getSessionUser;
import static spark.sql.catalog.ndb.TypeUtil.schemaHasCharNType;

public class VastScanBuilder
        implements SupportsPushDownV2Filters, SupportsPushDownRequiredColumns,
        SupportsPushDownLimit, SupportsPushDownAggregates
{
    private static final Logger LOG = LoggerFactory.getLogger(
            VastScanBuilder.class);
    private static final SecureRandom scanBuilderIdProvider = new SecureRandom();
    private static final ImmutableList<List<VastPredicate>> EMPTY_LIST = ImmutableList.of();

    private final VastTable table;
    private final int scanBuilderID = scanBuilderIdProvider.nextInt();
    private List<List<VastPredicate>> pushedDownPredicates;
    private Map<String, String> columnMasks;
    private StructType schema;
    private Integer limit;
    private boolean enablePredicatePushdown = true;

    public VastScanBuilder(VastTable table, VastCatalogUtils vastCatalogUtils)
    {
        this.table = table;
        this.schema = table.schema();
        this.limit = null;
        this.pushedDownPredicates = calculateRowColumnSecurityPredicates(
                vastCatalogUtils);
        LOG.debug(
                "new VastScanBuilder({}, {}), scanBuilderID={}, pushedDownPredicates={}",
                table.name(), schema, scanBuilderID, pushedDownPredicates);
    }

    public static VastPredicate convertToVastPredicate(Expression catalystExpr,
            StructType schema)
    {
        V2ExpressionBuilder builder = new V2ExpressionBuilder(catalystExpr,
                true);
        Option<org.apache.spark.sql.connector.expressions.Expression> connectorExpression = builder.build();
        if (connectorExpression.isEmpty() || connectorExpression
                .get()
                .references().length < 1) {
            throw new IllegalArgumentException(
                    "Unsupported expression: " + catalystExpr);
        }
        FieldReference fieldReference = null;
        for (org.apache.spark.sql.connector.expressions.Expression child : connectorExpression
                .get()
                .children()) {
            LOG.debug("catalystExpr child: {}", child);
            if (child instanceof FieldReference) {
                fieldReference = (FieldReference) child;
                break;
            }
        }
        if (fieldReference == null || fieldReference.fieldNames().length != 1) {
            throw new IllegalArgumentException(
                    "Unsupported expression: " + catalystExpr);
        }
        int fieldIndex = schema.fieldIndex(fieldReference.fieldNames()[0]);
        StructField field = schema.fields()[fieldIndex];
        return new VastPredicate((Predicate) connectorExpression.get(),
                connectorExpression.get().references()[0], field);
    }

    private ImmutableList<List<VastPredicate>> calculateRowColumnSecurityPredicates(
            VastCatalogUtils vastCatalogUtils)
    {
        LOG.debug("calculateRowColumnSecurityPredicates");
        String username = getSessionUser(vastCatalogUtils.getConfig());
        RowColumnSecurityResponse rowColumnSecurity = vastCatalogUtils.getRowColumnSecurity(
                table.getSchemaName(), table.getTableMD().tableName, username);
        LOG.debug("username={}, table={}/{}, rowColumnSecurity={}", username,
                table.getSchemaName(), table.getTableMD().tableName,
                rowColumnSecurity);
        ImmutableList<List<VastPredicate>> ret = EMPTY_LIST;
        try {
            if (rowColumnSecurity != null) {
                if (!rowColumnSecurity.getRowFilters().isEmpty()) {
                    ParserInterface parser = SparkSession
                            .active()
                            .sessionState()
                            .sqlParser();
                    ImmutableList.Builder<List<VastPredicate>> rowLevelPredicateBuilder = ImmutableList.builder();
                    for (String filter : rowColumnSecurity.getRowFilters()) {
                        org.apache.spark.sql.catalyst.expressions.Expression expr = parser.parseExpression(
                                filter);
                        VastPredicate vp = convertToVastPredicate(expr, schema);
                        List<VastPredicate> rowLevelPredicates = ImmutableList.of(
                                vp);
                        LOG.info(
                                "{}:{} adding row level security pushdown predicate: {}",
                                table.name(), scanBuilderID,
                                rowLevelPredicates);
                        rowLevelPredicateBuilder.add(rowLevelPredicates);
                    }
                    ret = rowLevelPredicateBuilder.build();
                }
                columnMasks = rowColumnSecurity.getMaskedColumns() != null ?
                        rowColumnSecurity.getMaskedColumns() :
                        Collections.emptyMap();
            }
        }
        catch (Exception e) {
            LOG.warn(
                    "failed to parse row level security filter, skipping row level security pushdown",
                    e);
            throw new IllegalArgumentException(
                    "failed to parse row level security filter", e);
        }
        return ret;
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
                pushedDownPredicates, columnMasks);
    }

    @Override
    public void pruneColumns(StructType requiredSchema)
    {
        LOG.info("{}:{} pruneColumns: {}", table.name(), scanBuilderID,
                requiredSchema);
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
                .map(f -> currFieldsByNameMap.get(f.getName()))
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
}
