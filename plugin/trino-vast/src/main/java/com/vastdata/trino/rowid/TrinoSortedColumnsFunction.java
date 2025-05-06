package com.vastdata.trino.rowid;

import com.vastdata.client.rowid.VastSortedColumnsFunction;
import com.vastdata.trino.VastTableHandle;

import java.util.List;

class TrinoSortedColumnsFunction implements VastSortedColumnsFunction<VastTableHandle>
{
    @Override
    public List<String> apply(VastTableHandle vastTableHandle)
    {
        return vastTableHandle.getSortedColumns().orElse(null);
    }
}
