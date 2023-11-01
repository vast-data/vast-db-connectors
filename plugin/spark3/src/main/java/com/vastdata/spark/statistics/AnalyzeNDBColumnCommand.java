/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import com.vastdata.client.stats.VastStatistics;
import com.vastdata.spark.VastTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.ResolvedTable;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeMap;
import org.apache.spark.sql.catalyst.expressions.AttributeMap$;
import org.apache.spark.sql.catalyst.plans.logical.AnalyzeColumn;
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.apache.spark.sql.execution.LeafExecNode;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.command.CommandUtils$;
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.Map;
import scala.collection.immutable.Seq;
import scala.collection.immutable.Seq$;
import scala.collection.mutable.Builder;
import scala.math.BigInt;

import java.util.HashMap;
import java.util.Optional;

import static com.vastdata.spark.statistics.StatsUtils.getVastClient;
import static java.lang.String.format;

public class AnalyzeNDBColumnCommand
        extends V2CommandExec
        implements LeafExecNode
{
    private static final Logger LOG = LoggerFactory.getLogger(AnalyzeNDBColumnCommand.class);


    private final ResolvedTable relation;
    private final VastTable table;

    private AnalyzeNDBColumnCommand(ResolvedTable relation) {
        super();
        this.relation = relation;
        this.table = (VastTable) relation.table();
    }

    @Override
    public Seq<Attribute> output()
    {
        return (Seq<Attribute>) scala.collection.immutable.Seq$.MODULE$.<Attribute>empty();
    }

    @Override
    public Seq<SparkPlan> children() {
        return (Seq<SparkPlan>) scala.collection.immutable.Seq$.MODULE$.<SparkPlan>empty();
    }

    @Override
    public SparkPlan withNewChildrenInternal(IndexedSeq<SparkPlan> newChildren)
    {
        return null;
    }

    @Override
    public Seq<InternalRow> run() {
        SparkSession session = session();
        LogicalPlan rel = session.table(relation.name()).logicalPlan();
        Seq<Attribute> columns = rel.output();
        LOG.info("Running analyze command with columns: {}", columns);
        Tuple2<Object, Map<Attribute, ColumnStat>> calculatedStats = CommandUtils$.MODULE$.computeColumnStats(session, rel, columns);
        long rowCount = (Long) calculatedStats._1;
        VastStatistics tableStats = StatsUtils.getTableLevelStats(getVastClient(), this.table.getTableMD().schemaName, this.table.getTableMD().tableName);
        BigInt sizeInBytes = BigInt.apply(tableStats.getSizeInBytes());
        LOG.info("Fetched table level statistics for table {}, statistics: numRows={}, sizeInBytes={}",
                this.table.getTableMD().tableName, tableStats.getNumRows(), tableStats.getSizeInBytes());
        AttributeMap<ColumnStat> columnStats = getColumnStatAttributeMap(calculatedStats._2);
        Statistics stats = new Statistics(sizeInBytes, Option.apply(BigInt.apply(rowCount)), columnStats, false);
        SparkVastStatisticsManager.getInstance().setTableStatistics(table, stats);
        LOG.info("Saved statistics for table {} to spark persistent statistics: {}, {}", this.table.name(), stats.simpleString(), stats.attributeStats());
        if (LOG.isDebugEnabled()) {
            stats.attributeStats().foreach(tup -> {
                Attribute attribute = tup._1;
                ColumnStat columnStat = tup._2;
                LOG.debug("Statistics for column: {}", attribute);
                Object maxValue = getValue(columnStat.max());
                if (maxValue == null) {
                    LOG.debug("max = null");
                }
                else {
                    LOG.debug("max = {} of class: {}", maxValue, maxValue.getClass());
                }
                Object minValue = getValue(columnStat.min());
                if (minValue == null) {
                    LOG.debug("min = null");
                }
                else {
                    LOG.debug("min = {} of class: {}", minValue, minValue.getClass());
                }
                return null;
            });
        }
        return (Seq<InternalRow>) Seq$.MODULE$.<InternalRow>newBuilder().result();
    }

    private Object getValue(Option<?> valueOption)
    {
        return valueOption.getOrElse(() -> null);
    }

    @Nullable
    private AttributeMap<ColumnStat> getColumnStatAttributeMap(Map<Attribute, ColumnStat> newColStatsMap)
    {
        Optional<Statistics> oldStats = SparkVastStatisticsManager.getInstance().getTableStatistics(table);
        if (oldStats.isPresent()) {
            HashMap<String, Tuple2<Attribute, ColumnStat>> mergeMap = new HashMap<>();
            newColStatsMap.foreach(statTup -> {
                Attribute att = statTup._1;
                ColumnStat stat = statTup._2;
                mergeMap.put(att.name(), Tuple2.apply(att, stat));
                return null;
            });
            // Currently even when ANALYZE FOR COLUMNS col1, .., colX is called, all columns are analyzed. This code is for future-proofness, to support only partial table stats
            AttributeMap<ColumnStat> oldColStatsMap = oldStats.get().attributeStats();
            oldColStatsMap.foreach(statTup -> {
                Attribute att = statTup._1;
                ColumnStat stat = statTup._2;
                Tuple2<Attribute, ColumnStat> previousKeyMapping = mergeMap.putIfAbsent(att.name(), Tuple2.apply(att, stat));
                if (previousKeyMapping == null) {
                    LOG.debug("Added previously analyzed column to stats {}:{} = {}", att.name(), att.exprId(), stat);
                }
                return null;
            });
            Builder<Tuple2<Attribute, ColumnStat>, List<Tuple2<Attribute, ColumnStat>>> mapBuilder = List$.MODULE$.newBuilder();
            mergeMap.forEach((name, tuple) -> mapBuilder.$plus$eq(tuple));
            return AttributeMap$.MODULE$.apply(mapBuilder.result());
        }
        else {
            return AttributeMap$.MODULE$.apply(newColStatsMap);
        }
    }

    @Override
    public boolean canEqual(Object that) {
        return that instanceof AnalyzeNDBTableCommand;
    }

    @Override
    public Object productElement(int n) {
        return this;
    }

    @Override
    public int productArity() {
        return 0;
    }

    public static AnalyzeNDBColumnCommand instance(AnalyzeColumn plan)
    {
        // TODO: support "ANALYZE TABLE t COMPUTE STATISTICS FOR COLUMNS c1,...cN" (see AnalyzeColumn#columnNames)
        LogicalPlan child = plan.child();
        if (child instanceof ResolvedTable) {
            return new AnalyzeNDBColumnCommand((ResolvedTable) child);
        }
        else {
            throw new RuntimeException(format("Unexpected child plan type: %s", plan));
        }
    }
}
