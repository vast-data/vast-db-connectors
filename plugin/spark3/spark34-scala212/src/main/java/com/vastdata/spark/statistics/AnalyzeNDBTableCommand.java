/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import com.vastdata.client.stats.VastStatistics;
import com.vastdata.client.error.VastExceptionFactory;
import com.vastdata.spark.VastTable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.ResolvedTable;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryCommand;
import org.apache.spark.sql.execution.LeafExecNode;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.IndexedSeq;
import scala.collection.Seq;
import scala.collection.Seq$;

import java.util.Collections;

import static com.vastdata.spark.statistics.StatsUtils.vastTableStatsToCatalystStatistics;
import static java.lang.String.format;

public class AnalyzeNDBTableCommand
    extends V2CommandExec
    implements LeafExecNode
{
    private static final Logger LOG = LoggerFactory.getLogger(AnalyzeNDBTableCommand.class);

    private final VastTable table;

    private AnalyzeNDBTableCommand(VastTable table) {
        super();
        this.table = table;
    }

    @Override
    public Seq<Attribute> output()
    {
        return Seq$.MODULE$.<Attribute>newBuilder().result();
    }

    @Override
    public Seq<SparkPlan> children() {
        return Seq$.MODULE$.<SparkPlan>newBuilder().result();
    }

    @Override
    public SparkPlan withNewChildrenInternal(IndexedSeq<SparkPlan> newChildren) {
        return null;
    }

    @Override
    public Seq<InternalRow> run() {
        LOG.debug("Running Analyze table command for table: {} in schema {}", this.table.name(), this.table.getSchemaName());
        try {
            // compute statistics via RPC
            VastStatistics tableStats = StatsUtils.getTableLevelStats(StatsUtils.getVastClient(), this.table.getTableMD().schemaName, this.table.getTableMD().tableName);
            //populate the cache
            org.apache.spark.sql.catalyst.plans.logical.Statistics newStats = vastTableStatsToCatalystStatistics(tableStats);
            SparkVastStatisticsManager.getInstance().setTableStatistics(table, newStats);
            LOG.debug("Saved statistics for table {} to spark persistent statistics: {}", table.name(), newStats.simpleString());
        } catch (Exception e) {
            throw VastExceptionFactory.toRuntime(e);
        }
        return Seq$.MODULE$.<InternalRow>newBuilder().result();
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

    public static AnalyzeNDBTableCommand instance(UnaryCommand plan)
    {
        LogicalPlan child = plan.child();
        if (child instanceof ResolvedTable) {
            ResolvedTable resolvedTable = (ResolvedTable) child;
            LOG.debug("Instantiating plan for AnalyzeNDBTable command for table: {}", resolvedTable.table().toString());
            return new AnalyzeNDBTableCommand((VastTable) resolvedTable.table());
        }
        else {
            throw new RuntimeException(format("Unexpected child plan type: %s", plan));
        }
    }
}
