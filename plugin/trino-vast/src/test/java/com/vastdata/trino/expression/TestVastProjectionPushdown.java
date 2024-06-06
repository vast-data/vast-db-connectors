/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.expression;

import io.airlift.slice.Slices;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestVastProjectionPushdown
{
    private static final VastProjectionPushdown pushdown = new VastProjectionPushdown(null);

    private static Optional<VastExpression> apply(ConnectorExpression expression)
    {
        return pushdown.apply(expression).map(result -> result.getPushedDown().get(0));
    }

    @Test
    public void testUnsupported()
    {
        assertThat(apply(new Constant(1, INTEGER))).isEmpty();
    }

    @Test
    public void testUnary()
    {
        ConnectorExpression expression = new Call(BOOLEAN, IS_NULL_FUNCTION_NAME, List.of(new Variable("x", INTEGER)));
        assertThat(apply(expression).map(VastExpression::getSymbol))
                .isEqualTo(Optional.of("is_null(x)"));

        expression = new Call(BOOLEAN, NOT_FUNCTION_NAME, List.of(expression));
        assertThat(apply(expression).map(VastExpression::getSymbol))
                .isEqualTo(Optional.of("is_valid(x)"));

        expression = new Call(BOOLEAN, NOT_FUNCTION_NAME, List.of(expression));
        assertThat(apply(expression).map(VastExpression::getSymbol))
                .isEmpty();
    }

    @Test
    public void testComparison()
    {
        ConnectorExpression expression = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(
                new Variable("x", INTEGER),
                new Constant(123L, INTEGER)));
        assertThat(apply(expression).map(VastExpression::getSymbol))
                .isEqualTo(Optional.of("equal(x,123)"));

        expression = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(
                new Variable("s", VARCHAR),
                new Constant(Slices.utf8Slice("ABC"), VARCHAR)));
        assertThat(apply(expression).map(VastExpression::getSymbol))
                .isEqualTo(Optional.of("equal(s,ABC)"));

        expression = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(
                new Variable("s", VARCHAR),
                new Constant(Slices.utf8Slice(""), VARCHAR)));
        assertThat(apply(expression).map(VastExpression::getSymbol))
                .isEqualTo(Optional.of("equal(s,)"));

        expression = new Call(BOOLEAN, IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, List.of(
                new Variable("x", INTEGER),
                new Variable("y", INTEGER)));
        assertThat(apply(expression).map(VastExpression::getSymbol))
                .isEmpty();
    }
}
