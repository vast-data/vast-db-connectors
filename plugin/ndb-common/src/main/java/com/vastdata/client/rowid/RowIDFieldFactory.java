package com.vastdata.client.rowid;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.function.Function;

public abstract class RowIDFieldFactory<T>
        implements Function<T, Field>
{
    private final RowIDStrategyTypeFactory<T> rowIDStrategyTypeFactory;

    public RowIDFieldFactory(RowIDStrategyTypeFactory<T> rowIDStrategyTypeFactory)
    {
        this.rowIDStrategyTypeFactory = rowIDStrategyTypeFactory;
    }

    @Override
    public Field apply(T base)
    {
        RowIDStrategyType strategyType = rowIDStrategyTypeFactory.apply(base);
        return RowIDStrategyFactory.get(strategyType).get();
    }
}
