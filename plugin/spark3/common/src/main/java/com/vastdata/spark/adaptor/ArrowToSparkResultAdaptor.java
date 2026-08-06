/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.adaptor;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class ArrowToSparkResultAdaptor<B>
{
    private final Class<?> columnVectorClass;
    private final Function<FieldVector, ?> columnMapper;
    private final Constructor<B> batchConstructor;

    public ArrowToSparkResultAdaptor(Class<B> batchClass,
            Class<?> columnVectorClass, Function<FieldVector, ?> columnMapper)
    {
        this.columnVectorClass = requireNonNull(columnVectorClass);
        this.columnMapper = requireNonNull(columnMapper);
        try {
            batchConstructor = batchClass.getConstructor(
                    Array.newInstance(columnVectorClass, 0).getClass(),
                    int.class);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public B adapt(VectorSchemaRoot result)
    {
        try {
            Object[] columnVectors = result.getFieldVectors().stream().map(
                    columnMapper).toArray(
                    size -> (Object[]) Array.newInstance(columnVectorClass,
                            size));
            return batchConstructor.newInstance(columnVectors,
                    result.getRowCount());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
