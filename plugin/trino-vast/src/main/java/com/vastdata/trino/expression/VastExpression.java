/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.vastdata.client.ArrowComputeFunction;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.stream.Collectors;
import java.util.Objects;

import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.nonNull;

public class VastExpression
{
    private final ArrowComputeFunction function; // `null` means identity function
    private final String variableName;
    private final Type variableType;
    private final List<Block> args;  // `Block` is used for literal value & type serialization across coordinator & workers (similar to `SortedRangeSet`)
    private final String symbol; // used for naming the new Trino column handle

    public static VastExpression identity(Variable variable)
    {
        return from(null, variable, List.of(), null);
    }

    public static VastExpression from(ArrowComputeFunction function, Variable variable)
    {
        return from(function, variable, List.of(), null);
    }

    public static VastExpression from(ArrowComputeFunction function, Variable variable, List<Constant> args, ConnectorSession session)
    {
        String name = variable.getName();
        Type type = variable.getType();
        List<Block> blocks = args
                .stream()
                .map(constant -> nativeValueToBlock(constant.getType(), constant.getValue()))
                .collect(Collectors.toList());
        // construct new projected column name: "func(variable[,constants...])"
        // it will allow de-duplication in case the same projection appears in multiple expressions.
        StringBuilder builder = new StringBuilder();
        if (nonNull(function)) {
            builder.append(function.getName()).append('(').append(variable.getName());
            for (Block block : blocks) {
                builder.append(',').append(variable.getType().getObjectValue(session, block, 0));
            }
            builder.append(')');
        }
        else {
            builder.append(variable.getName());
        }
        return new VastExpression(function, name, type, blocks, builder.toString());
    }

    @JsonCreator
    public VastExpression(
            @JsonProperty("function") ArrowComputeFunction function,
            @JsonProperty("variableName") String variableName,
            @JsonProperty("variableType") Type variableType,
            @JsonProperty("args") List<Block> args,
            @JsonProperty("symbol") String symbol)
    {
        this.function = function;
        this.variableName = variableName;
        this.variableType = variableType;
        this.args = args;
        this.symbol = symbol;
    }

    @JsonProperty
    public Type getVariableType()
    {
        return variableType;
    }

    public Type getResultType()
    {
        return BOOLEAN;
    }

    @JsonProperty
    public ArrowComputeFunction getFunction()
    {
        return function;
    }

    @JsonProperty
    public String getVariableName()
    {
        return variableName;
    }

    @JsonProperty
    public List<Block> getArgs()
    {
        return args;
    }

    @JsonProperty
    public String getSymbol()
    {
        return symbol;
    }

    @Override
    public String toString()
    {
        return symbol;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VastExpression that = (VastExpression) o;
        return Objects.equals(symbol, that.symbol);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(symbol);
    }
}
