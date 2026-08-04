/*
 *  Copyright (C) Vast Data Ltd.
 */
package ndb;

import com.vastdata.client.error.VastUserException;
import org.apache.spark.sql.catalyst.expressions.CurrentUser;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.PartialFunction;

/**
 * This rule replaces CurrentUser expressions with the actual user from the
 * session.
 */
public class SetUserOnSession
        extends Rule<LogicalPlan>
{
    private static final Logger LOG = LoggerFactory.getLogger(
            SetUserOnSession.class);

    public SetUserOnSession()
    {
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan)
    {
        return plan.transformExpressionsDown(new PartialFunction<>()
        {
            @Override
            public boolean isDefinedAt(Expression exp)
            {
                return exp instanceof CurrentUser;
            }

            @Override
            public Expression apply(Expression exp)
            {
                return handleCurrentUserExpression(exp);
            }
        });
    }

    private Expression handleCurrentUserExpression(Expression expression)
    {
        if (expression instanceof CurrentUser) {
            String user;
            try {
                user = NDBSparkSessionExtension.getSessionUser(NDB.getConfig());
            }
            catch (VastUserException e) {
                LOG.warn("failed on plan:", e);
                throw new RuntimeException(e);
            }
            if (user != null) {
                LOG.info("setting user {}", user);
            }

            Object literalValue = (user == null) ? null : UTF8String.fromString(
                    user);
            return new Literal(literalValue, DataTypes.StringType);
        }
        // If it's any other expression, return it unchanged.
        return expression;
    }
}
