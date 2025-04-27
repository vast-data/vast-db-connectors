/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.spark.statistics.AnalyzeNDBColumnCommand;
import com.vastdata.spark.statistics.AnalyzeNDBTableCommand;
import org.apache.spark.sql.catalyst.plans.logical.AnalyzeColumn;
import org.apache.spark.sql.catalyst.plans.logical.AnalyzeTable;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData;
import org.apache.spark.sql.catalyst.plans.logical.ShowColumns;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.SparkStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.List;
import scala.collection.mutable.Builder;

import static com.vastdata.spark.VastDelete.DELETE_ERROR_SUPPLIER;

public class NDBStrategy extends SparkStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger(NDBStrategy.class);
    public static final List<SparkPlan> EMPTY_RESULT_SEQ = List.<SparkPlan>newBuilder().result();

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

    private scala.collection.immutable.Seq<SparkPlan> planToResultImmutableSeq(SparkPlan plan)
    {
        Builder<SparkPlan, List<SparkPlan>> builder = List.newBuilder();
        builder.$plus$eq(plan);
        return builder.result();
    }
}
