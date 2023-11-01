/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.vastdata.client.error.VastUserException;
import com.vastdata.spark.predicate.VastPredicate;
import com.vastdata.spark.predicate.VastPredicatePushdown;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import spark.sql.catalog.ndb.TypeUtil;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.spark.ReportTruePostFilterOption.getOptionMap;
import static java.lang.String.format;
import static java.util.Objects.isNull;

public class VastScanBuilder
        implements SupportsPushDownV2Filters, SupportsPushDownRequiredColumns, SupportsPushDownLimit, SupportsPushDownAggregates
{
    private static final Logger LOG = LoggerFactory.getLogger(VastScanBuilder.class);
    private static final SecureRandom scanBuilderIdProvider = new SecureRandom();
    private static final Function1<StructField, Object> schemaHasCharNType = field -> field.dataType() instanceof CharType;
    private static final ImmutableList<List<VastPredicate>> EMPTY_LIST = ImmutableList.of();

    private final boolean reportTruePostFilter;
    private final VastTable table;
    private List<List<VastPredicate>> pushedDownPredicates;
    private StructType schema;

    private Integer limit;
    private final int scanBuilderID = scanBuilderIdProvider.nextInt();

    public VastScanBuilder(VastTable table, CaseInsensitiveStringMap options)
    {
        this(table, table.schema(), null, options);
    }

    private VastScanBuilder(VastTable table, StructType schema, Integer limit, CaseInsensitiveStringMap options)
    {
        this.table = table;
        this.schema = schema;
        this.limit = limit;
        this.pushedDownPredicates = EMPTY_LIST;
        LOG.debug("new VastScanBuilder({}, {}, {}), scanBuilderID={}", table.name(), schema, options, scanBuilderID);
        reportTruePostFilter = options != null && options.containsKey("reportTruePostFilter") && Boolean.parseBoolean(options.get("reportTruePostFilter"));
    }

    @Override
    public Scan build()
    {
        LOG.debug("{}:{} build new VastScan", table.name(), scanBuilderID);
        try {
            return new VastScan(scanBuilderID, table, schema, limit, pushedDownPredicates);
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public void pruneColumns(StructType requiredSchema)
    {
        LOG.info("{}:{} pruneColumns: {}", table.name(), scanBuilderID, requiredSchema);
        // Adaptation is needed because spark uses string type for char(n) columns - get original column type
        this.schema = !this.schema.exists(schemaHasCharNType) ? requiredSchema : adaptRequiredSchemaToTableSchema(this.schema, requiredSchema);
    }

    private StructType adaptRequiredSchemaToTableSchema(StructType currSchema, StructType requiredSchema)
    {
        List<Field> currFields = TypeUtil.sparkSchemaToArrowFieldsList(currSchema);
        Map<String, Field> currFieldsByNameMap = Maps.uniqueIndex(currFields, Field::getName);
        List<Field> collectedRequiredFields = TypeUtil.sparkSchemaToArrowFieldsList(requiredSchema)
                .stream()
                .map(f -> currFieldsByNameMap.get(f.getName()))
                .collect(Collectors.toList());
        return TypeUtil.arrowFieldsListToSparkSchema(collectedRequiredFields);
    }

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates)
    {
        LOG.info("{}:{} PREDICATES pushdown: current={}, new={}", table.name(), scanBuilderID, pushedDownPredicates, predicates);
        if (pushedDownPredicates != null && !pushedDownPredicates.isEmpty()) {
            throw new IllegalStateException("Second pushdown is not supported");
        }
        VastPredicatePushdown result = VastPredicatePushdown.parse(predicates, schema);
        pushedDownPredicates = result.getPushedDown();
        return reportTruePostFilter ? result.getPostFilter().toArray(new Predicate[0]) : predicates; // actual post-filtering is made in physical plan optimizations (NDBStrategy)
    }

    @Override
    public Predicate[] pushedPredicates()
    {
        BinaryOperator<Predicate> oriBinPred = (p1, p2) -> new Predicate("OR", new Predicate[] {p1, p2});
        return this.pushedDownPredicates
                .stream()
                .map(predList ->
                        predList
                                .stream()
                                .map(VastPredicate::getPredicate)
                                .reduce(oriBinPred)
                                .orElseThrow(() -> new VerifyException("Predicate not preset"))
                        )
                .toArray(Predicate[]::new);
    }

    @Override
    public boolean pushLimit(int limit)
    {
        LOG.info("{}:{} LIMIT pushdown: current={}, limit={}", table.name(), scanBuilderID, this.limit, limit);
        this.limit = isNull(this.limit) ? limit : Math.min(this.limit, limit);
        return false; // Since a single split can enforce only its own "local" limit, we need Spark to apply a "global" limit.
        // For example, if we pushdown `LIMIT 1`, each split will return a single row, and Spark should return exactly one of them to the user.
    }

    @Override
    public boolean supportCompletePushDown(Aggregation aggregation)
    {
        LOG.info("{}:{} supportCompletePushDown: {}", table.name(), scanBuilderID, describeAggregation(aggregation));
        return SupportsPushDownAggregates.super.supportCompletePushDown(aggregation);
    }

    @Override
    public boolean pushAggregation(Aggregation aggregation)
    {
        LOG.info("{}:{} pushAggregation: {}", table.name(), scanBuilderID, describeAggregation(aggregation));
        return false;
    }

    private String describeAggregation(Aggregation aggregation)
    {
        return format("func:%s, group_by:%s", Arrays.toString(aggregation.aggregateExpressions()), Arrays.toString(aggregation.groupByExpressions()));
    }

    public static VastScanBuilder rebuildScan(VastScan scan)
    {
        return new VastScanBuilder(scan.getTable(), scan.readSchema(), scan.getLimit(), getOptionMap());
    }
}
