package com.vastdata.client.rowid;

import java.util.List;

public final class SortedColumnsBasedRowIDTypeFactory
{
    private SortedColumnsBasedRowIDTypeFactory() {}

    public static <T> RowIDStrategyType get(VastSortedColumnsFunction<T> sortedColumnsExtractor, T obj)
    {
        List<String> sortedColumns = sortedColumnsExtractor.apply(obj);
        if (sortedColumns == null || sortedColumns.isEmpty()) {
            return RowIDStrategyType.UNSIGNED_INT64;
        }
        else {
            return RowIDStrategyType.DECIMAL_128;
        }
    }
}
