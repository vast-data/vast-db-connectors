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
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ShowColumns;
import org.apache.spark.sql.catalyst.plans.logical.TableSpec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.SparkStrategy;
import org.apache.spark.sql.execution.datasources.v2.CreateTableAsSelectExec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.List;
import scala.collection.mutable.Builder;
import spark.sql.catalog.ndb.InitializedVastCatalog;
import spark.sql.catalog.ndb.VastCatalogCreateColumnOverride;

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
        } else if (plan instanceof CreateTableAsSelect) {
            CreateTableAsSelect c = (CreateTableAsSelect) plan;
            CreateTableAsSelectExec createTableAsSelectExec =
                    new CreateTableAsSelectExec(new VastCatalogCreateColumnOverride(InitializedVastCatalog.getVastCatalog()),
                            c.tableName(), c.partitioning(), c.query(), (TableSpec) c.tableSpec(), c.writeOptions(), c.ignoreIfExists());
            LOG.debug("CreateTableAsSelectExec: {}", createTableAsSelectExec);
            return planToResultImmutableSeq(createTableAsSelectExec);
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
        LOG.debug("{} not supported by NDBStrategy", plan.getClass().getCanonicalName());
        return EMPTY_RESULT_SEQ;
    }

    private scala.collection.immutable.Seq<SparkPlan> planToResultImmutableSeq(SparkPlan plan)
    {
        Builder<SparkPlan, List<SparkPlan>> builder = List.newBuilder();
        builder.addOne(plan);
        return builder.result();
    }
}
