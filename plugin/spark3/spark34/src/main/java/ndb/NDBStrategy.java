/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.spark.statistics.AnalyzeNDBColumnCommand;
import com.vastdata.spark.statistics.AnalyzeNDBTableCommand;
import ndb.view.AlterNDBViewAsCommand;
import ndb.view.AlterNDBViewAsPlan;
import ndb.view.CreateNDBViewCommand;
import ndb.view.CreateNDBViewPlan;
import ndb.view.DropNDBViewCommand;
import ndb.view.DropNDBViewPlan;
import ndb.view.RenameNDBViewCommand;
import ndb.view.RenameNDBViewPlan;
import ndb.view.ShowNDBViewsCommand;
import ndb.view.ShowNDBViewsPlan;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.AnalyzeColumn;
import org.apache.spark.sql.catalyst.plans.logical.AnalyzeTable;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData;
import org.apache.spark.sql.catalyst.plans.logical.ShowColumns;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.SparkStrategy;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.List;
import scala.collection.mutable.Builder;

import static com.vastdata.spark.VastDelete.DELETE_ERROR_SUPPLIER;

public class NDBStrategy extends SparkStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger(NDBStrategy.class);
    public static final List<SparkPlan> EMPTY_RESULT_SEQ = List.<SparkPlan>newBuilder().result();
    private final SparkSession session;

    public NDBStrategy(SparkSession session) {
        this.session = session;
    }

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
        else if (plan instanceof DeleteFromTable) {
            DeleteFromTable deleteFromTable = (DeleteFromTable) plan;
            LogicalPlan deleteChild = deleteFromTable.child();
            if (deleteChild instanceof DataSourceV2ScanRelation) {
                DataSourceV2ScanRelation v2ScanRelation = (DataSourceV2ScanRelation) deleteChild;
                LOG.info("Deleting from datasource v2 relation={}, table={}", v2ScanRelation.relation(), v2ScanRelation.relation().table());
            }
            else {
                LOG.error("Unexpected child for delete command: {}", deleteChild);
                throw new IllegalArgumentException("Unexpected child for delete command: " + deleteChild.getClass());
            }
        }
        else if (plan instanceof ReplaceData) { // when there is a filter that is not supported for pushdown, cos(x) for example
            ReplaceData replaceData = (ReplaceData) plan;
            LogicalPlan child = replaceData.child();
            if (child instanceof Filter) {
                Filter filter = (Filter) child;
                throw DELETE_ERROR_SUPPLIER.apply(filter.condition());
            }
        } else if (plan instanceof ShowNDBViewsPlan) {
            final ShowNDBViewsCommand command = ShowNDBViewsCommand.instance((ShowNDBViewsPlan) plan);
            LOG.info("Returning {} for ShowNDBViewsPlan: {}", command, plan);
            return planToResultImmutableSeq(command);
        } else if (plan instanceof CreateNDBViewPlan) {
            final CreateNDBViewCommand command = CreateNDBViewCommand.instance((CreateNDBViewPlan) plan);
            LOG.info("Returning {} for CreateNDBViewPlan: {}", command, plan);
            return planToResultImmutableSeq(command);
        } else if (plan instanceof DropNDBViewPlan) {
            final DropNDBViewCommand command = DropNDBViewCommand.instance((DropNDBViewPlan) plan);
            LOG.info("Returning {} for DropNDBViewPlan: {}", command, plan);
            return planToResultImmutableSeq(command);
        } else if (plan instanceof RenameNDBViewPlan) {
            final RenameNDBViewCommand command = RenameNDBViewCommand.instance((RenameNDBViewPlan) plan, session);
            LOG.info("Returning {} for RenameNDBViewPlan: {}", command, plan);
            return planToResultImmutableSeq(command);
        } else if (plan instanceof AlterNDBViewAsPlan) {
            final AlterNDBViewAsCommand command = AlterNDBViewAsCommand.instance((AlterNDBViewAsPlan) plan, session);
            LOG.info("Returning {} for AlterNDBViewAsPlan: {}", command, plan);
            return planToResultImmutableSeq(command);
        }
        LOG.info("{} not supported by NDBStrategy", plan.getClass().getCanonicalName());
        return EMPTY_RESULT_SEQ;
    }

    private scala.collection.immutable.Seq<SparkPlan> planToResultImmutableSeq(SparkPlan plan)
    {
        Builder<SparkPlan, List<SparkPlan>> builder = List.newBuilder();
        builder.addOne(plan);
        return builder.result();
    }
}
