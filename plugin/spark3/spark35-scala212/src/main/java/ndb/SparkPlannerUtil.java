/*
 *  Copyright (C) Vast Data Ltd.
 */
package ndb;

import com.vastdata.spark.VastTable;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.Metadata;
import scala.Option;
import scala.collection.Seq;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.Seq$;
import scala.collection.mutable.ArrayBuffer;
import scala.collection.mutable.Builder;
import spark.sql.catalog.ndb.VastCatalog;

import java.util.ArrayList;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static ndb.view.NDBTablesResolutionRule.VAST_THROW_RCLS_ERROR;
import static spark.sql.catalog.ndb.NDBRowLevelOperationIdentifier.adaptTableIdentifiersToRowLevelOp;
import static spark.sql.catalog.ndb.NDBRowLevelOperationIdentifier.isForRowLevelOp;
import static spark.sql.catalog.ndb.NDBRowLevelOperationIdentifier.trimTableNameFromRowLevelOpSuffix;

public final class SparkPlannerUtil
{
    public static final Seq<String> EMPTY_STRING_SEQ = (Seq<String>) Seq$.MODULE$.empty();
    public static final scala.collection.immutable.Seq<SparkPlan> EMPTY_RESULT_SEQ = (scala.collection.immutable.Seq<SparkPlan>) scala.collection.immutable.Seq$.MODULE$.empty();
    public static final Seq<LogicalPlan> EMPTY_LOGICAL_PLAN_SEQ = (Seq<LogicalPlan>) Seq$.MODULE$.<LogicalPlan>empty();

    private SparkPlannerUtil()
    {
    }

    public static Alias newAlias(Expression expression, String aliasName,
            ExprId exprId)
    {
        return new Alias(expression, aliasName, exprId, EMPTY_STRING_SEQ,
                Option.apply(Metadata.empty()), EMPTY_STRING_SEQ);
    }

    public static Alias newAlias(Expression expression, String aliasName)
    {
        return newAlias(expression, aliasName, NamedExpression.newExprId());
    }

    public static DataSourceV2Relation newDataSourceV2Relation(VastTable table,
            VastCatalog vastCatalog, String[] namespace, String name)
    {
        return DataSourceV2Relation.create(table, Option.apply(vastCatalog),
                Option.apply(Identifier.of(namespace, name)));
    }

    public static UnresolvedRelation removeVastResolutionSuffixes(
            UnresolvedRelation unresolvedRel)
    {
        Seq<String> origIdentifiers = unresolvedRel.multipartIdentifier();
        Seq<String> adaptedIdentifiers = nameSeqWithoutRCLSSuffix(
                origIdentifiers);
        adaptedIdentifiers = removeRowLevelOpSuffixFromNameSeq(
                adaptedIdentifiers);
        return unresolvedRel.copy(adaptedIdentifiers, unresolvedRel.options(),
                unresolvedRel.isStreaming());
    }

    public static UnresolvedRelation addVastResolutionSuffixes(
            UnresolvedRelation unresolvedRel, boolean rowLevelOp,
            boolean rowColSecurity)
    {
        Seq<String> origIdentifiers = unresolvedRel.multipartIdentifier();
        Seq<String> adaptedIdentifiers = origIdentifiers;
        if (rowLevelOp) {
            java.util.List<String> origIdentList = new ArrayList<>(
                    origIdentifiers.size());
            IntStream.range(0, origIdentifiers.size()).forEachOrdered(
                    i -> origIdentList.add(origIdentifiers.apply(i)));
            Builder<String, List<String>> newIdentifiersBuilder = List$.MODULE$.newBuilder();
            Consumer<String> resultConsumer = newIdentifiersBuilder::$plus$eq;
            adaptTableIdentifiersToRowLevelOp(origIdentList, resultConsumer);
            adaptedIdentifiers = newIdentifiersBuilder.result();
        }
        if (rowColSecurity) {
            adaptedIdentifiers = nameSeqWithRCLSSuffix(adaptedIdentifiers);
        }
        return unresolvedRel.copy(adaptedIdentifiers, unresolvedRel.options(),
                unresolvedRel.isStreaming());
    }

    private static Seq<String> nameSeqWithRCLSSuffix(
            Seq<String> multipartIdentifier)
    {
        ArrayBuffer<String> buf = new ArrayBuffer<>();
        int i = 0;
        for (; i < multipartIdentifier.size() - 1; i++) {
            buf.$plus$eq(multipartIdentifier.apply(i));
        }
        buf.$plus$eq(multipartIdentifier.apply(i) + VAST_THROW_RCLS_ERROR);
        return buf.toSeq();
    }

    public static Seq<String> nameSeqWithoutRCLSSuffix(
            Seq<String> multipartIdentifier)
    {
        ArrayBuffer<String> buf = new ArrayBuffer<>();
        int i = 0;
        for (; i < multipartIdentifier.size() - 1; i++) {
            buf.$plus$eq(multipartIdentifier.apply(i));
        }
        String name = multipartIdentifier.apply(i);
        int indexOfRclsSuffix = name.indexOf(VAST_THROW_RCLS_ERROR);
        if (indexOfRclsSuffix > 0) {
            buf.$plus$eq(name.substring(0, indexOfRclsSuffix));
        }
        else {
            buf.$plus$eq(name);
        }
        return buf.toSeq();
    }

    public static Seq<String> removeRowLevelOpSuffixFromNameSeq(
            Seq<String> tableNameSeq)
    {
        String tableName = tableNameSeq.last();
        if (isForRowLevelOp(tableName)) {
            ArrayBuffer<String> nameSeqWithoutRCLSSuffix = new ArrayBuffer<>();
            for (int i = 0; i < tableNameSeq.size() - 1; i++) {
                nameSeqWithoutRCLSSuffix.$plus$eq(tableNameSeq.apply(i));
            }
            nameSeqWithoutRCLSSuffix.$plus$eq(
                    trimTableNameFromRowLevelOpSuffix(tableName));
            return nameSeqWithoutRCLSSuffix.toSeq();
        }
        return tableNameSeq;
    }
}
