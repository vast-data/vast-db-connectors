/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public final class ReportTruePostFilterOption
{
    private static final CaseInsensitiveStringMap map = initMap();

    private static CaseInsensitiveStringMap initMap()
    {
        return new CaseInsensitiveStringMap(ImmutableMap.of("reportTruePostFilter", "true"));
    }

    private ReportTruePostFilterOption() {}

    public static CaseInsensitiveStringMap getOptionMap()
    {
        return map;
    }
}
