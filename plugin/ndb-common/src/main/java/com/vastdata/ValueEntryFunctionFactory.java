/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata;

import java.util.function.BiConsumer;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;

public final class ValueEntryFunctionFactory
{
    private ValueEntryFunctionFactory() {}

    public static <T> ValueEntrySetter<T> newSetter(BiConsumer<Integer, T> valueConsumer, IntConsumer nullConsumer)
    {
        return new ValueEntrySetter<T>() {
            @Override
            public void setNull(int i)
            {
                nullConsumer.accept(i);
            }

            @Override
            public void set(int i, T val)
            {
                valueConsumer.accept(i, val);
            }
        };
    }

    public static <T> ValueEntryGetter<T> newGetter(IntFunction<T> valueSupplier, IntFunction<Boolean> nullSupplier, IntFunction<Boolean> parentNullSupplier)
    {
        return new ValueEntryGetter<T>() {
            @Override
            public boolean isNull(int i)
            {
                return nullSupplier.apply(i);
            }

            @Override
            public boolean isParentNull(int i)
            {
                return parentNullSupplier.apply(i);
            }

            @Override
            public T get(int i)
            {
                return valueSupplier.apply(i);
            }
        };
    }
}
