/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.ParsedURL;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.stats.StatisticsUrlExtractor;
import org.apache.spark.sql.connector.catalog.Table;

import static com.vastdata.spark.VastTable.HANDLE_ID_PROPERTY;
import static java.lang.String.format;

public class SparkStatisticsUrlExtractor
        extends StatisticsUrlExtractor<Table>
{
    private static final SparkStatisticsUrlExtractor instance = new SparkStatisticsUrlExtractor();

    private SparkStatisticsUrlExtractor()
    {
        super(t -> {
            try {
                return ParsedURL.of(t.name()).getBucket();
            }
            catch (VastUserException e) {
                throw new RuntimeException(format("Failed extracting bucket from table: %s", t.name()), e);
            }
        },
                t1 -> t1.properties().get(HANDLE_ID_PROPERTY));
    }

    public static StatisticsUrlExtractor<Table> instance()
    {
        return instance;
    }
}
