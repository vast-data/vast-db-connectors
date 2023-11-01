/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.stats;

public class StatisticsUrl
{
    private static final String VAST_STATISTICS_FILE_SUFFIX = "stats";
    private final String bucket;
    private final String key;

    private StatisticsUrl(String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
    }

    public String getBucket()
    {
        return bucket;
    }

    public String getKey()
    {
        return key;
    }

    protected static String getTableStatisticsIdentifier(String handleIdString, String tag) {
        return String.join(".", handleIdString, tag, VAST_STATISTICS_FILE_SUFFIX);
    }

    public static <T> StatisticsUrl extract(T target, StatisticsUrlExtractor<T> helper, String tag)
    {
        String bucket = helper.getBucket(target);
        String handleID = helper.getHandleID(target);
        String tableStatisticsIdentifier = getTableStatisticsIdentifier(handleID, tag);
        return new StatisticsUrl(bucket, tableStatisticsIdentifier);
    }
}
