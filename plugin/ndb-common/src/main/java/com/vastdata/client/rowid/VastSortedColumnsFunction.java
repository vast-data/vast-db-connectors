package com.vastdata.client.rowid;

import java.util.List;
import java.util.function.Function;

public interface VastSortedColumnsFunction<T>
        extends Function<T, List<String>>
{
}
