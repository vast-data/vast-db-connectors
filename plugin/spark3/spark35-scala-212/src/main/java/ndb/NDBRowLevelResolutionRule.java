/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.AssignmentUtils;
import org.apache.spark.sql.catalyst.analysis.FieldName;
import org.apache.spark.sql.catalyst.analysis.ResolvedFieldName;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.AddColumns;
import org.apache.spark.sql.catalyst.plans.logical.Assignment;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.DropColumns;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.QualifiedColType;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceColumns;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.catalyst.util.CharVarcharUtils;
import org.apache.spark.sql.catalyst.util.CharVarcharUtils$;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.PartialFunction;
import scala.PartialFunction$;
import scala.collection.Traversable;
import scala.collection.generic.CanBuildFrom;
import scala.collection.immutable.List;
import scala.collection.Seq;
import scala.collection.immutable.List$;
import scala.collection.mutable.Builder;
import scala.runtime.AbstractFunction1;

import java.util.stream.IntStream;

import static java.lang.String.format;
import static spark.sql.catalog.ndb.TypeUtil.SPARK_ROW_ID_FIELD;

public class NDBRowLevelResolutionRule
        extends org.apache.spark.sql.catalyst.rules.Rule<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan>
{
    private static final Logger LOG = LoggerFactory.getLogger(NDBRowLevelResolutionRule.class);

    @Override
    public LogicalPlan apply(LogicalPlan plan)
    {
        if (plan instanceof UpdateTable) {
            UpdateTable u = (UpdateTable) plan;
            if (u.resolved() && u.rewritable() && !u.aligned()) {
                if (SparkSession.getActiveSession().get().conf().contains("spark.sql.storeAssignmentPolicy") &&
                        SparkSession.getActiveSession().get().conf().get("spark.sql.storeAssignmentPolicy").equalsIgnoreCase("legacy")) {
                    throw new RuntimeException("LEGACY store assignment policy is disallowed in Spark data source V2. " +
                            "Please set the configuration spark.sql.storeAssignmentPolicy to other values.");
                }
                Function1<LogicalPlan, LogicalPlan> func = lp -> {
                    if (lp instanceof DataSourceV2Relation) {
                        final DataSourceV2Relation v2Relation = (DataSourceV2Relation) lp;
                        final CanBuildFrom<Seq<?>, AttributeReference, Seq<AttributeReference>> builder = Seq.canBuildFrom();
                        final Seq<AttributeReference> newOutput = v2Relation
                                .output()
                                .map(new AbstractFunction1<AttributeReference, AttributeReference>() {
                                         @Override
                                         public AttributeReference apply(AttributeReference v1) {
                                             return CharVarcharUtils$.MODULE$.cleanAttrMetadata(v1);
                                         }
                                     },
                                        new CanBuildFrom<Traversable<AttributeReference>, AttributeReference, Seq<AttributeReference>>() {
                                            public Builder<AttributeReference, Seq<AttributeReference>> apply() {
                                                return builder.apply();
                                            }

                                            public Builder<AttributeReference, Seq<AttributeReference>> apply(Traversable<AttributeReference> from) {
                                                return builder.apply(from.toList());
                                            }
                                        })
                                .toSeq();
                        LOG.info("NDBResolutionRule UpdateTable: new output: {}", newOutput);
                        return (LogicalPlan) v2Relation.copy(v2Relation.table(), newOutput, v2Relation.catalog(), v2Relation.identifier(), v2Relation.options());
                    }
                    else {
                        return lp;
                    }
                };
                PartialFunction<LogicalPlan, LogicalPlan> transformer = PartialFunction$.MODULE$.apply(func);
                LogicalPlan transformedTable = u.table().transform(transformer);
                Seq<Assignment> newAssignments = AssignmentUtils.alignUpdateAssignments(transformedTable.output(), u.assignments());
                return u.copy(transformedTable, newAssignments, u.condition());
            }
        }
        else if (plan instanceof DeleteFromTable) {
            DeleteFromTable d = (DeleteFromTable) plan;
            Function1<LogicalPlan, LogicalPlan> func = lp -> {
                if (lp instanceof DataSourceV2Relation) {
                    DataSourceV2Relation v2Relation = (DataSourceV2Relation) lp;
                    Builder<AttributeReference, List<AttributeReference>> refsWithRowID = List$.MODULE$.newBuilder();
                    AttributeReference rowIdAttRef = new AttributeReference(SPARK_ROW_ID_FIELD.name(), SPARK_ROW_ID_FIELD.dataType(), false, Metadata.empty(), ExprId.apply(0), List$.MODULE$.<String>newBuilder().result());
                    v2Relation.output().foreach(refsWithRowID::$plus$eq);
                    refsWithRowID.$plus$eq(rowIdAttRef);
                    List<AttributeReference> newOutput = refsWithRowID.result();
                    LOG.info("NDBResolutionRule DeleteFromTable: new output: {}", newOutput);
                    return (LogicalPlan) v2Relation.copy(v2Relation.table(), newOutput, v2Relation.catalog(), v2Relation.identifier(), v2Relation.options());
                }
                else {
                    return lp;
                }
            };
            PartialFunction<LogicalPlan, LogicalPlan> transformer = PartialFunction$.MODULE$.apply(func);
            LogicalPlan transformedTable = d.table().transform(transformer);
            DeleteFromTable copy = d.copy(transformedTable, d.condition());
            LOG.debug("DeleteFromTable: {}", copy);
        }
        else if (plan instanceof DropColumns) {
            LOG.debug("Drop columns: {}", plan);
            DropColumns drop = (DropColumns) plan;
            Seq<FieldName> columns = drop.columnsToDrop();
            IntStream.range(0, columns.size()).forEach(i -> {
                FieldName fName = columns.apply(i);
                if (fName instanceof ResolvedFieldName) {
                    ResolvedFieldName resolvedFieldName = (ResolvedFieldName) fName;
                    String name = resolvedFieldName.field().name();
                    if (SPARK_ROW_ID_FIELD.name().equalsIgnoreCase(name)) {
                        throw new RuntimeException(format("Dropping %s is not allowed", name));
                    }
                }
            });
        }
        else if (plan instanceof AddColumns) {
            LOG.debug("Add columns: {}", plan);
            AddColumns drop = (AddColumns) plan;
            Seq<QualifiedColType> columns = drop.columnsToAdd();
            IntStream.range(0, columns.size()).forEach(i -> {
                QualifiedColType fName = columns.apply(i);
                String name = fName.colName();
                if (SPARK_ROW_ID_FIELD.name().equalsIgnoreCase(name)) {
                    throw new RuntimeException(format("Adding %s is not allowed", name));
                }
            });
        }
        else if (plan instanceof ReplaceColumns) {
            ReplaceColumns replaceColumns = (ReplaceColumns) plan;
            Seq<QualifiedColType> colsToAdd = replaceColumns.columnsToAdd();
            IntStream.range(0, colsToAdd.size()).forEach(i -> {
                String name = colsToAdd.apply(i).colName();
                if (SPARK_ROW_ID_FIELD.name().equalsIgnoreCase(name)) {
                    throw new RuntimeException(format("Adding %s is not allowed", name));
                }
            });
        }
        return plan;
    }
}
