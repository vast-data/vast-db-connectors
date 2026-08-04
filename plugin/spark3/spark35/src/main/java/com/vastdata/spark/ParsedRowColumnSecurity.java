/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.vastdata.client.RowColumnSecurityResponse;
import com.vastdata.spark.predicate.VastPredicate;
import com.vastdata.spark.predicate.VastPredicatePushdown;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.util.V2ExpressionBuilder;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import spark.sql.catalog.ndb.VastCatalogUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static ndb.NDBSparkSessionExtension.getSessionUser;

public final class ParsedRowColumnSecurity
{
    private static final Logger LOG = LoggerFactory.getLogger(
            ParsedRowColumnSecurity.class);

    private final List<List<VastPredicate>> pushedPredicates;
    private final List<Expression> postFilter;
    private final Map<String, Expression> columnMasks;
    private final Set<String> allowedColumns;
    private final Set<String> deniedColumns;

    private ParsedRowColumnSecurity(List<List<VastPredicate>> pushedPredicates,
            List<Expression> postFilter, Map<String, Expression> columnMasks,
            Set<String> allowedColumns, Set<String> deniedColumns)
    {
        this.pushedPredicates = nullableFunction(ImmutableList::copyOf,
                pushedPredicates);
        this.postFilter = nullableFunction(ImmutableList::copyOf, postFilter);
        this.columnMasks = nullableFunction(ImmutableMap::copyOf, columnMasks);
        this.allowedColumns = nullableFunction(ImmutableSet::copyOf,
                allowedColumns);
        this.deniedColumns = nullableFunction(ImmutableSet::copyOf,
                deniedColumns);
        LOG.debug(
                "ParsedRowColumnSecurity pushedPredicates: {}, postFilter: {}, columnMasks: {}",
                pushedPredicates, postFilter, columnMasks);
    }

    private static <T> T nullableFunction(Function<T, T> f, T object)
    {
        if (object == null) {
            return null;
        }
        return f.apply(object);
    }

    private static ParsedRowColumnSecurity parseRowColumnSecurity(
            RowColumnSecurityResponse rowColumnSecurity, StructType schema,
            boolean vastPushdown)
    {
        ImmutableList.Builder<List<VastPredicate>> pushedPredicatesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Expression> postfilterBuilder = ImmutableList.builder();
        ImmutableMap.Builder<String, Expression> columnMasksBuilder = ImmutableMap.builder();
        Set<String> allowedColumns = null;
        Set<String> deniedColumns = null;
        try {
            if (rowColumnSecurity != null) {
                allowedColumns = rowColumnSecurity.getAllowedColumns();
                deniedColumns = rowColumnSecurity.getDeniedColumns();
                if (!rowColumnSecurity.getRowFilters().isEmpty()) {
                    ParserInterface sqlParser = SparkSession
                            .active()
                            .sessionState()
                            .sqlParser();
                    for (String filter : rowColumnSecurity.getRowFilters()) {
                        org.apache.spark.sql.catalyst.expressions.Expression rawExpr = sqlParser.parseExpression(
                                filter);
                        Option<org.apache.spark.sql.connector.expressions.Expression> build =
                                vastPushdown ?
                                        new V2ExpressionBuilder(rawExpr,
                                                true).build() :
                                        null;
                        if (vastPushdown && !build.isEmpty()) {
                            org.apache.spark.sql.connector.expressions.Expression connectorExpr = build.get();
                            Predicate[] asPredicate = {(Predicate) connectorExpr};
                            VastPredicatePushdown result = VastPredicatePushdown.parse(
                                    asPredicate, schema);
                            List<List<VastPredicate>> pushedDown = result.getPushedDown();
                            List<Predicate> postFilter = result.getPostFilter();
                            LOG.debug(
                                    "after pushdown: pushedDown={}, postFilter={}",
                                    pushedDown, postFilter);
                            pushedPredicatesBuilder.addAll(pushedDown);
                            if (postFilter != null && !postFilter.isEmpty()) {
                                postfilterBuilder.add(rawExpr);
                            }
                        }
                        else {
                            postfilterBuilder.add(rawExpr);
                        }
                    }
                }
                Map<String, String> maskedColumns = rowColumnSecurity.getMaskedColumns();
                if (maskedColumns != null) {
                    LOG.info("DBG maskedColumns: {}", maskedColumns);
                    ParserInterface sqlParser = SparkSession
                            .active()
                            .sessionState()
                            .sqlParser();
                    for (Map.Entry<String, String> entry : maskedColumns.entrySet()) {
                        String maskString = entry.getValue();
                        org.apache.spark.sql.catalyst.expressions.Expression rawExpr = sqlParser.parseExpression(
                                maskString);
                        LOG.info("DBG rawExpr of class {}: {}",
                                rawExpr.getClass(), rawExpr);
                        columnMasksBuilder.put(entry.getKey(), rawExpr);
                    }

                }
            }
        }
        catch (Exception e) {
            LOG.warn(
                    "failed to parse row level security filter, skipping row level security pushdown",
                    e);
            throw new IllegalArgumentException(
                    "failed to parse row level security filter", e);
        }
        return new ParsedRowColumnSecurity(pushedPredicatesBuilder.build(),
                postfilterBuilder.build(), columnMasksBuilder.build(),
                allowedColumns, deniedColumns);
    }

    public static ParsedRowColumnSecurity getParsedRowColumnSecurity(
            VastCatalogUtils vastCatalogUtils, String schemaName,
            String tableName, StructType schema, boolean vastPushdown)
    {
        String username = getSessionUser(vastCatalogUtils.getConfig());
        RowColumnSecurityResponse rowColumnSecurity = vastCatalogUtils.getRowColumnSecurity(
                schemaName, tableName, username);
        LOG.debug("username={}, table={}/{}, rowColumnSecurity={}", username,
                schemaName, tableName, rowColumnSecurity);
        return ParsedRowColumnSecurity.parseRowColumnSecurity(rowColumnSecurity,
                schema, vastPushdown);
    }

    public List<List<VastPredicate>> getPushedPredicates()
    {
        return pushedPredicates;
    }

    public List<Expression> getPostFilter()
    {
        return postFilter;
    }

    public Map<String, Expression> getColumnMasks()
    {
        return columnMasks;
    }

    public Set<String> getAllowedColumns()
    {
        return allowedColumns;
    }

    public Set<String> getDeniedColumns()
    {
        return deniedColumns;
    }
}
