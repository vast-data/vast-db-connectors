package com.vastdata.client.rowid;

import java.util.function.Function;

public interface RowIDStrategyTypeFactory<T>
        extends Function<T, RowIDStrategyType>
{
}
