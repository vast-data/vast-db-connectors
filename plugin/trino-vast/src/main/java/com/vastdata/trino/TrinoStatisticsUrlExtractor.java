/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.ParsedURL;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.stats.StatisticsUrlExtractor;

import static java.lang.String.format;

public class TrinoStatisticsUrlExtractor extends StatisticsUrlExtractor<VastTableHandle>
{
    private static final TrinoStatisticsUrlExtractor instance = new TrinoStatisticsUrlExtractor();

    private TrinoStatisticsUrlExtractor()
    {
        super(t -> {
            try {
                return ParsedURL.of(t.getSchemaName()).getBucket();
            }
            catch (VastUserException e) {
                throw new RuntimeException(format("Failed extracting bucket from table: %s", t.getTableName()), e);
            }
        }, VastTableHandle::getHandleID);
    }

    public static StatisticsUrlExtractor<VastTableHandle> instance()
    {
        return instance;
    }
}
