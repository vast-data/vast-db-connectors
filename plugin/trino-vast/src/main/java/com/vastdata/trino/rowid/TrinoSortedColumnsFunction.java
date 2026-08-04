/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.rowid;

import com.vastdata.client.rowid.VastSortedColumnsFunction;
import com.vastdata.trino.VastTableHandle;

import java.util.List;

class TrinoSortedColumnsFunction
        implements VastSortedColumnsFunction<VastTableHandle>
{
    @Override
    public List<String> apply(VastTableHandle vastTableHandle)
    {
        if (vastTableHandle.getSortedColumns().isPresent()) {
            return vastTableHandle.getSortedColumns().orElseThrow();
        }
        if (vastTableHandle.getPartitionColumns().isPresent()) {
            return vastTableHandle.getPartitionPostTransformColumnNames().orElseThrow();
        }
        return null;
    }
}
