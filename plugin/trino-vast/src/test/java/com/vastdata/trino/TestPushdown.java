/* Copyright (C) Vast Data Ltd. */

package com.vastdata.trino;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vastdata.trino.VastMetadata.tryParseSubstringMatch;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPushdown
{
    @Test
    public void testParseSubstringMatch()
    {
        Variable variable = new Variable("i", VARCHAR);
        VastColumnHandle column = VastColumnHandle.fromField(Field.nullable(variable.getName(), ArrowType.Utf8.INSTANCE));
        Map<String, ColumnHandle> assignments = Map.of(variable.getName(), column);

        ConnectorExpression expr = Constant.TRUE;
        assertThat(tryParseSubstringMatch(expr, Set.of(), assignments)).isEqualTo(Optional.empty());

        // no support for `IS NULL` expression
        expr = new Call(BOOLEAN, IS_NULL_FUNCTION_NAME, List.of(variable));
        assertThat(tryParseSubstringMatch(expr, Set.of(), assignments)).isEqualTo(Optional.empty());

        // no support for escaped LIKE expression
        expr = new Call(BOOLEAN, LIKE_FUNCTION_NAME, List.of(variable, constant("%123%"), constant("#")));
        assertThat(tryParseSubstringMatch(expr, Set.of(), assignments)).isEqualTo(Optional.empty());

        // substring LIKE expression
        expr = new Call(BOOLEAN, LIKE_FUNCTION_NAME, List.of(variable, constant("%123%")));
        VastSubstringMatch match = new VastSubstringMatch(column, "123");
        assertThat(tryParseSubstringMatch(expr, Set.of(), assignments)).isEqualTo(Optional.of(match));
        assertThat(tryParseSubstringMatch(expr, Set.of("another"), assignments)).isEqualTo(Optional.of(match));
        assertThat(tryParseSubstringMatch(expr, Set.of(variable.getName()), assignments)).isEqualTo(Optional.empty());
    }

    @DataProvider
    public Object[][] unsupportedPatterns()
    {
        return new Object[][] {
                {"%123"},
                {"123%"},
                {"%1\\2\\3%"},
                {"%%"},
                {"%%"},
                {"%"},
                {""},
                {"%12%34%"},
                {"%12_34%"},
        };
    }

    @Test(dataProvider = "unsupportedPatterns")
    public void testParseUnsupportedPatterns(String pattern)
    {
        Variable variable = new Variable("i", VARCHAR);
        Call expr = new Call(BOOLEAN, LIKE_FUNCTION_NAME, List.of(
                variable, constant(pattern)));
        assertThat(tryParseSubstringMatch(expr, Set.of(), Map.of())).isEqualTo(Optional.empty());
    }

    private static Constant constant(String value)
    {
        return new Constant(utf8Slice(value), VARCHAR);
    }
}
