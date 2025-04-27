/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import ndb.view.AlterNDBViewAsPlan;
import ndb.view.CreateNDBViewPlan;
import ndb.view.DropNDBViewPlan;
import ndb.view.RenameNDBViewPlan;
import ndb.view.ShowNDBViewsPlan;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.AlterViewAs;
import org.apache.spark.sql.catalyst.plans.logical.CreateView;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.DropView;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.RenameTable;
import org.apache.spark.sql.catalyst.plans.logical.ShowViews;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.PartialFunction;
import scala.collection.immutable.Seq;
import scala.collection.immutable.Seq$;

import static spark.sql.catalog.ndb.NDBRowLevelOperationIdentifier.adaptTableIdentifiersToRowLevelOp;

public class NDBParser
        implements ParserInterface
{
    private static final Logger LOG = LoggerFactory.getLogger(NDBParser.class);

    public static final Seq<LogicalPlan> EMPTY_LOGICAL_PLAN_SEQ = (Seq<LogicalPlan>) Seq$.MODULE$.<LogicalPlan>empty();

    private final SparkSession session;
    private final ParserInterface parser;

    public NDBParser(SparkSession sparkSession, ParserInterface parserInterface) {
        session = sparkSession;
        parser = parserInterface;
    }

    @Override
    public LogicalPlan parsePlan(String sqlText)
            throws ParseException
    {
        LogicalPlan original = parser.parsePlan(sqlText);
        if (original instanceof DeleteFromTable || original instanceof UpdateTable) {
            LOG.debug("NDBParser.parsePlan original LogicalPlan is {}: {}", original.getClass(), original);
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
            LogicalPlan transformed = original.transform(transformer);
            LOG.info("Transformed row level operation plan: {}", transformed);
            return transformed;
        } else if (original instanceof ShowViews) {
            LOG.debug("NDBParser.parsePlan original LogicalPlan is a ShowViews plan");
            return ShowNDBViewsPlan.instance((ShowViews) original);
        } else if (original instanceof CreateView) {
            LOG.debug("NDBParser.parsePlan original LogicalPlan is a CreateView plan, with child: {}", ((CreateView) original).child());
            final String currentCatalog = session.sessionState().catalogManager().currentCatalog().name();
            final String[] currentNamespace = session.sessionState().catalogManager().currentNamespace();
            return CreateNDBViewPlan.instance((CreateView) original, currentCatalog, currentNamespace);
        } else if (original instanceof DropView) {
            LOG.debug("NDBParser.parsePlan original LogicalPlan is a DropView plan");
            return DropNDBViewPlan.instance((DropView) original);
        } else if (original instanceof AlterViewAs) {
            LOG.debug("NDBParser.parsePlan original LogicalPlan is an AlterViewAs plan");
            return AlterNDBViewAsPlan.instance((AlterViewAs) original);
        } else if (original instanceof RenameTable && ((RenameTable) original).isView()) {
            LOG.debug("NDBParser.parsePlan original LogicalPlan is an RenameTable(isView=True) plan");
            return RenameNDBViewPlan.instance((RenameTable) original);
        }
        return original;
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
