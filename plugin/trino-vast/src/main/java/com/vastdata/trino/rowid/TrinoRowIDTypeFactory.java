package com.vastdata.trino.rowid;

import com.vastdata.client.rowid.RowIDStrategyType;
import com.vastdata.client.rowid.RowIDStrategyTypeFactory;
import com.vastdata.client.rowid.SortedColumnsBasedRowIDTypeFactory;
import com.vastdata.trino.VastTableHandle;

class TrinoRowIDTypeFactory implements RowIDStrategyTypeFactory<VastTableHandle>
{
    private static final TrinoSortedColumnsFunction TRINO_SORTED_COLUMNS_FUNCTION = new TrinoSortedColumnsFunction();

    private TrinoRowIDTypeFactory() {}

    public static final TrinoRowIDTypeFactory INSTANCE = new TrinoRowIDTypeFactory();

    @Override
    public RowIDStrategyType apply(VastTableHandle vastTableHandle)
    {
        return SortedColumnsBasedRowIDTypeFactory.get(TRINO_SORTED_COLUMNS_FUNCTION, vastTableHandle);
    }
}
