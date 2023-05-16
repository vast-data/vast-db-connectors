/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.arrow.computeir.flatbuf.Call;
import org.apache.arrow.computeir.flatbuf.Expression;
import org.apache.arrow.computeir.flatbuf.ExpressionImpl;

import static com.vastdata.client.ArrowComputeFunction.AND;
import static com.vastdata.client.ArrowComputeFunction.EQUAL;
import static com.vastdata.client.ArrowComputeFunction.GREATER;
import static com.vastdata.client.ArrowComputeFunction.GREATER_EQUAL;
import static com.vastdata.client.ArrowComputeFunction.IS_NULL;
import static com.vastdata.client.ArrowComputeFunction.IS_VALID;
import static com.vastdata.client.ArrowComputeFunction.LESS;
import static com.vastdata.client.ArrowComputeFunction.LESS_EQUAL;
import static com.vastdata.client.ArrowComputeFunction.MATCH_SUBSTRING;
import static com.vastdata.client.ArrowComputeFunction.OR;

public abstract class VastExpressionSerializer
        implements FlatBufferSerializer
{
    protected static final int NOT_SPECIFIED = 0;

    protected FlatBufferBuilder builder;

    private void setBuilder(FlatBufferBuilder builder)
    {
        this.builder = builder;    
    }
    
    @Override
    public int serialize(FlatBufferBuilder builder)
    {
        setBuilder(builder);
        return serialize();
    }

    protected abstract int serialize();

    protected int buildFunction(ArrowComputeFunction function, int... offsets)
    {
        int nameOffset = builder.createString(function.getName());
        Call.startArgumentsVector(builder, offsets.length);
        for (int i = offsets.length - 1; i >= 0; --i) {
            builder.addOffset(offsets[i]);
        }
        int argumentsOffset = builder.endVector();

        Call.startCall(builder);
        Call.addName(builder, nameOffset);
        Call.addArguments(builder, argumentsOffset);
        int callOffset = Call.endCall(builder);

        return Expression.createExpression(builder, ExpressionImpl.Call, callOffset);
    }

    protected int buildAnd(int... offsets)
    {
        return buildFunction(AND, offsets);
    }

    protected int buildOr(int... offsets)
    {
        return buildFunction(OR, offsets);
    }

    protected int buildEqual(int column, int literal)
    {
        return buildFunction(EQUAL, column, literal);
    }

    protected int buildLess(int column, int literal, boolean inclusive)
    {
        return buildFunction(inclusive ? LESS_EQUAL : LESS, column, literal);
    }

    protected int buildGreater(int column, int literal, boolean inclusive)
    {
        return buildFunction(inclusive ? GREATER_EQUAL : GREATER, column, literal);
    }

    protected int buildIsNull(int column)
    {
        return buildFunction(IS_NULL, column);
    }

    protected int buildIsValid(int column)
    {
        return buildFunction(IS_VALID, column);
    }

    protected int buildMatchSubstring(int column, int literal)
    {
        return buildFunction(MATCH_SUBSTRING, column, literal);
    }

    protected int buildColumn(long position)
    {
        int index = org.apache.arrow.computeir.flatbuf.FieldIndex.createFieldIndex(builder, position);
        int ref = org.apache.arrow.computeir.flatbuf.FieldRef.createFieldRef(builder, org.apache.arrow.computeir.flatbuf.Deref.FieldIndex, index, NOT_SPECIFIED /*relation*/);
        return Expression.createExpression(builder, ExpressionImpl.FieldRef, ref);
    }
}
