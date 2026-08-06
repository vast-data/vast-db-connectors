/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.client.error.ErrorType;
import com.vastdata.client.error.VastRuntimeException;
import org.apache.spark.sql.catalyst.AliasIdentifier;
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Function1;
import scala.Option;
import scala.PartialFunction;
import scala.collection.immutable.Seq;

import static java.lang.String.format;
import static ndb.view.NDBTablesResolutionRule.VAST_THROW_RCLS_ERROR;
import static spark.sql.catalog.ndb.NDBRowLevelOperationIdentifier.isForRowLevelOp;
import static spark.sql.catalog.ndb.NDBRowLevelOperationIdentifier.trimTableNameFromRowLevelOpSuffix;

public class NDBRCLSResolvedRelationAdaptorRule
        extends
        org.apache.spark.sql.catalyst.rules.Rule<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan>
{
    private static DataSourceV2Relation adaptRelation(
            DataSourceV2Relation dataSourceV2Relation)
    {
        Identifier identifier = dataSourceV2Relation.identifier().get();

        String adaptedName = identifier.name().endsWith(VAST_THROW_RCLS_ERROR) ?
                identifier.name().substring(0,
                        identifier.name().indexOf(VAST_THROW_RCLS_ERROR)) :
                identifier.name();
        if (isForRowLevelOp(adaptedName)) {
            adaptedName = trimTableNameFromRowLevelOpSuffix(adaptedName);
        }
        Table table = dataSourceV2Relation.table();
        Seq<AttributeReference> output = dataSourceV2Relation.output();
        Option<CatalogPlugin> catalog = dataSourceV2Relation.catalog();
        String[] namespace = identifier.namespace();
        Identifier adaptedIdent = Identifier.of(namespace, adaptedName);
        CaseInsensitiveStringMap options = dataSourceV2Relation.options();
        return dataSourceV2Relation.copy(table, output, catalog,
                Option.apply(adaptedIdent), options);
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan)
    {
        Function1<LogicalPlan, LogicalPlan> rclsRemover = p -> {
            if (p instanceof SubqueryAlias) {
                SubqueryAlias subqueryAlias = (SubqueryAlias) p;
                if (subqueryAlias.identifier().name().endsWith(
                        VAST_THROW_RCLS_ERROR)) {
                    return new SubqueryAlias(
                            adaptIdentifier(subqueryAlias.identifier()),
                            subqueryAlias.child());
                }
            }
            else if (p instanceof DataSourceV2Relation) {
                return adaptRelation((DataSourceV2Relation) p);
            }
            else if (p instanceof DeleteFromTable) {
                DeleteFromTable delete = (DeleteFromTable) p;
                LogicalPlan child = delete.child();
                if (!(child instanceof DataSourceV2Relation)) {
                    if (child instanceof Project) {
                        throw new VastRuntimeException(
                                "Delete from table is not allowed by current VAST security policy rules",
                                null, ErrorType.USER);
                    }
                    if (child instanceof Filter) {
                        Filter filterNode = (Filter) child;
                        And combinedFilter = new And(delete.condition(),
                                filterNode.condition());
                        return delete.copy(filterNode.child(), combinedFilter);
                    }
                    else if (child instanceof SubqueryAlias) {
                        SubqueryAlias subqueryAlias = (SubqueryAlias) child;
                        LogicalPlan subqueryChild = EliminateSubqueryAliases.apply(
                                subqueryAlias);
                        return delete.copy(subqueryChild, delete.condition());
                    }
                    else {
                        throw new VastRuntimeException(
                                format("Unexpected child class for %s: %s. plan: %s",
                                        p.getClass(), child.getClass(), p),
                                null, ErrorType.GENERAL);
                    }
                }
            }
            else if (p instanceof UpdateTable) {
                UpdateTable updateTable = (UpdateTable) p;
                LogicalPlan child = updateTable.child();
                if (child instanceof SubqueryAlias) {
                    child = EliminateSubqueryAliases.apply(child);
                }
                if (!(child instanceof DataSourceV2Relation)) {
                    if (child instanceof Project || child instanceof Filter) {
                        throw new VastRuntimeException(
                                "Update table is not allowed by current VAST security policy rules",
                                null, ErrorType.USER);
                    }
                    else {
                        throw new VastRuntimeException(
                                format("Unexpected child class for %s: %s",
                                        p.getClass(), child.getClass()), null,
                                ErrorType.GENERAL);
                    }

                }
            }
            return p;
        };
        return plan.transformUp(PartialFunction.fromFunction(rclsRemover));
    }

    private AliasIdentifier adaptIdentifier(AliasIdentifier identifier)
    {
        String adaptedName = identifier.name().substring(0,
                identifier.name().indexOf(VAST_THROW_RCLS_ERROR));
        Seq<String> qualifier = identifier.qualifier();
        return new AliasIdentifier(adaptedName, qualifier);
    }
}
