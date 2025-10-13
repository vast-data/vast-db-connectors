/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.PartialFunction;
import scala.collection.immutable.List;
import scala.collection.immutable.Seq;
import scala.collection.mutable.Builder;

import java.util.stream.IntStream;

import static spark.sql.catalog.ndb.NDBRowLevelOperationIdentifier.adaptTableNameToRowLevelOp;

public class NDBParser
        implements ParserInterface
{
    private static final Logger LOG = LoggerFactory.getLogger(NDBParser.class);

    private final ParserInterface parser;

    public NDBParser(ParserInterface parser)
    {
        this.parser = parser;
    }

    @Override
    public LogicalPlan parsePlan(String sqlText)
            throws ParseException
    {
        LogicalPlan plan = parser.parsePlan(sqlText);
        if (plan instanceof DeleteFromTable || plan instanceof UpdateTable) {
            Function1<LogicalPlan, LogicalPlan> func = p -> {
                if (p instanceof UnresolvedRelation) {
                    UnresolvedRelation unresolvedRel = (UnresolvedRelation) p;
                    Seq<String> adaptedIdentifiers = adaptTableIdentifiersToRowLevelOp(unresolvedRel.multipartIdentifier());
                    return unresolvedRel.copy(adaptedIdentifiers, unresolvedRel.options(), unresolvedRel.isStreaming());
                }
                else {
                     return p;
                }
            };
            PartialFunction<LogicalPlan, LogicalPlan> transformer = PartialFunction.fromFunction(func);
            LogicalPlan transformed = plan.transform(transformer);
            LOG.info("Transformed row level operation plan: {}", transformed);
            return transformed;
        }
        return plan;
    }

    private static Seq<String> adaptTableIdentifiersToRowLevelOp(Seq<String> origIdentifiers)
    {
        Builder<String, List<String>> newIdentifiersBuilder = List.newBuilder();
        IntStream.range(0, origIdentifiers.size() - 1).forEachOrdered(i -> newIdentifiersBuilder.$plus$eq(origIdentifiers.apply(i)));
        String adaptedTableName = adaptTableNameToRowLevelOp(origIdentifiers.apply(origIdentifiers.size() - 1));
        newIdentifiersBuilder.$plus$eq(adaptedTableName);
        return newIdentifiersBuilder.result();
    }

    @Override
    public Expression parseExpression(String sqlText)
            throws ParseException
    {
        return parser.parseExpression(sqlText);
    }

    @Override
    public TableIdentifier parseTableIdentifier(String sqlText)
            throws ParseException
    {
        return parser.parseTableIdentifier(sqlText);
    }

    @Override
    public FunctionIdentifier parseFunctionIdentifier(String sqlText)
            throws ParseException
    {
        return parser.parseFunctionIdentifier(sqlText);
    }

    @Override
    public Seq<String> parseMultipartIdentifier(String sqlText)
            throws ParseException
    {
        return parser.parseMultipartIdentifier(sqlText);
    }

    @Override
    public LogicalPlan parseQuery(String sqlText)
            throws ParseException
    {
        return parser.parseQuery(sqlText);
    }

    @Override
    public StructType parseTableSchema(String sqlText)
            throws ParseException
    {
        return parser.parseTableSchema(sqlText);
    }

    @Override
    public DataType parseDataType(String sqlText)
            throws ParseException
    {
        return parser.parseDataType(sqlText);
    }
}
