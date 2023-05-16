/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.expression;

import com.google.common.base.MoreObjects;
import com.vastdata.client.ArrowComputeFunction;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vastdata.client.ArrowComputeFunction.EQUAL;
import static com.vastdata.client.ArrowComputeFunction.GREATER;
import static com.vastdata.client.ArrowComputeFunction.GREATER_EQUAL;
import static com.vastdata.client.ArrowComputeFunction.IS_NULL;
import static com.vastdata.client.ArrowComputeFunction.IS_VALID;
import static com.vastdata.client.ArrowComputeFunction.LESS;
import static com.vastdata.client.ArrowComputeFunction.LESS_EQUAL;
import static com.vastdata.client.ArrowComputeFunction.NOT_EQUAL;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static java.util.Objects.nonNull;

public class VastProjectionPushdown
{
    private static final Logger LOG = Logger.get(VastProjectionPushdown.class);
    private static final Map<FunctionName, ArrowComputeFunction> supportedComparisons = Map.of(
            EQUAL_OPERATOR_FUNCTION_NAME, EQUAL,
            NOT_EQUAL_OPERATOR_FUNCTION_NAME, NOT_EQUAL,
            GREATER_THAN_OPERATOR_FUNCTION_NAME, GREATER,
            GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, GREATER_EQUAL,
            LESS_THAN_OPERATOR_FUNCTION_NAME, LESS,
            LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, LESS_EQUAL);

    private static final Set<FunctionName> pushThroughFunctions = Set.of(AND_FUNCTION_NAME, OR_FUNCTION_NAME);

    public static class Result {
        private final ConnectorExpression remaining;
        private final List<VastExpression> pushedDown;

        private Result(ConnectorExpression remaining, List<VastExpression> pushedDown)
        {
            this.remaining = remaining;
            this.pushedDown = pushedDown;
        }

        public static Result from(VastExpression expression)
        {
            Variable newVariable = new Variable(expression.toString(), expression.getVariableType());
            return new Result(newVariable, List.of(expression));
        }

        public ConnectorExpression getRemaining()
        {
            return remaining;
        }

        public List<VastExpression> getPushedDown()
        {
            return pushedDown;
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("remaining", remaining)
                    .add("pushedDown", pushedDown)
                    .toString();
        }
    }


    private final ConnectorSession session;

    public VastProjectionPushdown(ConnectorSession session)
    {
        this.session = session;
    }

    public Optional<Result> apply(ConnectorExpression expression)
    {
        LOG.debug("trying to pushdown %s", expression);
        Optional<Variable> result = matchUnaryCalls(expression, IS_NULL_FUNCTION_NAME);
        if (result.isPresent()) {
            Variable variable = result.get();
            return Optional.of(Result.from(VastExpression.from(IS_NULL, variable)));
        }
        result = matchUnaryCalls(expression, NOT_FUNCTION_NAME, IS_NULL_FUNCTION_NAME);
        if (result.isPresent()) {
            Variable variable = result.get();
            return Optional.of(Result.from(VastExpression.from(IS_VALID, variable)));
        }
        return matchComparison(expression);
    }

    public Optional<Result> matchComparison(ConnectorExpression expression)
    {
        if (!(expression instanceof Call)) {
            return Optional.empty();
        }

        Call call = (Call) expression;
        if (pushThroughFunctions.contains(call.getFunctionName()))
        {
            List<ConnectorExpression> args = call.getArguments();
            List<Result> results = new ArrayList(args.size());
            for (ConnectorExpression arg : args) {
                Optional<Result> result = apply(arg);
                if (result.isEmpty()) {
                    return Optional.empty();
                }
                results.add(result.get());
            }
            // rewrite new function call arguments
            args = results.stream().map(Result::getRemaining).collect(Collectors.toList());
            // concatenate all pushed-down expressions
            List<VastExpression> pushedDown = results
                    .stream()
                    .flatMap(result -> result.getPushedDown().stream())
                    .collect(Collectors.toList());
            return Optional.of(new Result(new Call(call.getType(), call.getFunctionName(), args), pushedDown));
        }

        ArrowComputeFunction function = supportedComparisons.get(call.getFunctionName());
        if (nonNull(function)) {
            ConnectorExpression left = call.getArguments().get(0);
            ConnectorExpression right = call.getArguments().get(1);
            if (!(left instanceof Variable && right instanceof Constant)) {
                return Optional.empty();
            }
            Variable variable = (Variable) left;
            Constant constant = (Constant) right;
            return Optional.of(Result.from(VastExpression.from(function, variable, List.of(constant), session)));
        }
        return Optional.empty();
    }

    static Optional<Variable> matchUnaryCalls(ConnectorExpression expression, FunctionName... names)
    {
        for (FunctionName name : names) {
            if (!(expression instanceof Call)) {
                return Optional.empty();
            }
            Call call = (Call) expression;
            if (!call.getFunctionName().equals(name)) {
                return Optional.empty();
            }
            expression = call.getArguments().get(0);
        }
        if (!(expression instanceof Variable)) {
            return Optional.empty();
        }
        return Optional.of((Variable) expression);
    }
}
