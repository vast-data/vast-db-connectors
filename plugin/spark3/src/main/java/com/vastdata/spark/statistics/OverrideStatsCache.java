/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeMap$;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.write.RowLevelOperationTable;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.immutable.List$;
import scala.collection.mutable.Builder;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.vastdata.spark.VastTable.DEFAULT_COLUMN_LEVEL_STATS;
import static com.vastdata.spark.statistics.AttributeUtil.replaceAttributeExpId;
import static java.lang.String.format;

public class OverrideStatsCache implements Consumer<LogicalPlan>
{
    private static final Logger LOG = LoggerFactory.getLogger(OverrideStatsCache.class);

    public void accept(LogicalPlan plan)
    {
        if (plan.statsCache().isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(format("Cache miss for plan: %s", plan), new Exception());
            }
            plan.foreachUp((node) -> {
                if (node instanceof DataSourceV2ScanRelation) {
                    LOG.info("Overriding statistics on plan {} - {} node: {} - {}", plan.getClass(), plan.hashCode(), node.getClass(), node.hashCode());
                    overrideStats((DataSourceV2ScanRelation) node);
                }
                else if (node instanceof DataSourceV2Relation) {
                    LOG.info("Overriding statistics on plan {} - {} node: {} - {}", plan.getClass(), plan.hashCode(), node.getClass(), node.hashCode());
                    overrideStats((DataSourceV2Relation) node);
                }
                return null;
            });
        }
        else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(format("Cache hit for plan: %s", plan), new Exception());
            }
        }
    }

    private Statistics updateAttrRef(Seq<AttributeReference> outputAttrs, Statistics originalStats)
    {
        Map<String, ExprId> attrsNameIdMap = new HashMap<>();
        outputAttrs.foreach(a -> attrsNameIdMap.put(a.name(), a.exprId()));

        Builder<Tuple2<Attribute, ColumnStat>, scala.collection.immutable.List<Tuple2<Attribute, ColumnStat>>> objectSeqBuilder = List$.MODULE$.newBuilder();

        originalStats.attributeStats().foreach(entry -> {
            Attribute newAttr = replaceAttributeExpId(entry._1, attr -> attrsNameIdMap.getOrDefault(attr.name(), attr.exprId()));
            return objectSeqBuilder.$plus$eq(Tuple2.apply(newAttr, entry._2));
        });

        scala.collection.immutable.Seq<Tuple2<Attribute, ColumnStat>> result = objectSeqBuilder.result();
        return new Statistics(
                originalStats.sizeInBytes(),
                originalStats.rowCount(),
                AttributeMap$.MODULE$.apply(result),
                false);
    }

    private void overrideStats(DataSourceV2ScanRelation tableScanPlan)
    {
        Optional<Statistics> tableStats = SparkVastStatisticsManager.getInstance().getTableStatistics(tableScanPlan.relation().table());
        Statistics modifiedTableStats = updateAttrRef(tableScanPlan.output(), tableStats.orElse(DEFAULT_COLUMN_LEVEL_STATS));
        if (LOG.isDebugEnabled()) {
            LOG.debug("Setting column statistics for DataSourceV2ScanRelation: {}\n{}", tableStats, tableStats.map(s -> s.attributeStats().mkString("\n")).orElse("Empty Stats cache"));
        }
        new SparkPlanStatisticsSetter().accept(tableScanPlan, modifiedTableStats);
        if (LOG.isDebugEnabled()) {
            Statistics statistics = tableScanPlan.statsCache().get();
            LOG.debug("New statistics cache for DataSourceV2ScanRelation plan {}: {}\n{}", tableScanPlan, statistics, statistics.attributeStats());
        }
    }

    private void overrideStats(DataSourceV2Relation tableScanPlan)
    {
        Table table = tableScanPlan.table();
        if (table instanceof RowLevelOperationTable) {
            table = ((RowLevelOperationTable) table).table();
        }
        if (!table.getClass().getCanonicalName().equals("com.vastdata.spark.VastTable")) {
            LOG.error("Unsupported table class: {}", table.getClass());
            throw new RuntimeException(format("Unsupported table class: %s", table.getClass()));
        }
        Optional<Statistics> tableStats = SparkVastStatisticsManager.getInstance().getTableStatistics(table);
        Statistics modifiedTableStats = updateAttrRef(tableScanPlan.output(), tableStats.orElse(DEFAULT_COLUMN_LEVEL_STATS));
        if (LOG.isDebugEnabled()) {
            LOG.debug("Setting column statistics for DataSourceV2Relation: {}\n{}", tableStats, tableStats.map(s -> s.attributeStats().mkString("\n")).orElse("Empty Stats cache"));
        }
        new SparkPlanStatisticsSetter().accept(tableScanPlan, modifiedTableStats);
        if (LOG.isDebugEnabled()) {
            Statistics statistics = tableScanPlan.statsCache().get();
            LOG.debug("New statistics cache for DataSourceV2Relation plan {}: {}\n{}", tableScanPlan, statistics, statistics.attributeStats());
        }
    }
}
