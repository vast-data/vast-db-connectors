/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.spark.VastTable;
import org.apache.spark.sql.catalyst.analysis.ResolvedTable;
import org.apache.spark.sql.catalyst.plans.logical.DropPartitions;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.catalyst.trees.TreeNodeTag;
import org.apache.spark.sql.connector.catalog.Identifier;

public class NonAcidResolutionRule
        extends Rule<LogicalPlan>
{
    public static final String VAST_ALLOW_NON_ACID = "vast.allow_non_acid";
    private static final TreeNodeTag<Boolean> ALLOW_TO_DROP_PARTITION = new TreeNodeTag<>(
            "ALLOW_TO_DROP_PARTITION");

    private static LogicalPlan removeAllowNonAcidFlagFromTableName(
            DropPartitions drop, ResolvedTable table)
    {
        LogicalPlan newPlan = drop.copy(table.copy(table.catalog(),
                        Identifier.of(table.identifier().namespace(),
                                table.identifier().name().split(" ")[0]), table.table(),
                        table.outputAttributes()), drop.parts(), drop.ifExists(),
                drop.purge());
        newPlan.setTagValue(ALLOW_TO_DROP_PARTITION, true);
        return newPlan;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan)
    {
        if (!(plan instanceof DropPartitions)) {
            return plan;
        }

        DropPartitions drop = (DropPartitions) plan;

        if (!(drop.table() instanceof ResolvedTable)) {
            return plan;
        }

        ResolvedTable table = (ResolvedTable) drop.table();

        if (!(table.table() instanceof VastTable)) {
            return plan;
        }

        if (plan.getTagValue(ALLOW_TO_DROP_PARTITION).nonEmpty()) {
            return plan;
        }

        if (!table.identifier().name().endsWith(VAST_ALLOW_NON_ACID)) {
            throw new RuntimeException(
                    "Beware, 'drop partition' is a non acid operation! In order to proceed please use `" + table
                            .identifier()
                            .name() + " vast.allow_non_acid` (with the backticks)");
        }

        return removeAllowNonAcidFlagFromTableName(drop, table);
    }
}
