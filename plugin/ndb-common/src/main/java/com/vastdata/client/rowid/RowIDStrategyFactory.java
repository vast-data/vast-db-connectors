package com.vastdata.client.rowid;

import java.util.EnumMap;

public class RowIDStrategyFactory
{
    private RowIDStrategyFactory() {}

    private static final Int64RowIDStrategy defaultStrategy = new Int64RowIDStrategy();

    private static final EnumMap<RowIDStrategyType, RowIDStrategy> implMap = new EnumMap<>(RowIDStrategyType.class);
    static {
        implMap.put(RowIDStrategyType.UNSIGNED_INT64, defaultStrategy);
        implMap.put(RowIDStrategyType.DECIMAL_128, new Decimal128RowIDStrategy());
    }
    public static RowIDStrategy get(RowIDStrategyType type)
    {
        return implMap.getOrDefault(type, defaultStrategy);
    }
}
