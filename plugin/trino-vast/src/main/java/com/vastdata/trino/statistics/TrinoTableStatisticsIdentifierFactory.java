/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.statistics;

import com.vastdata.client.ParsedURL;
import com.vastdata.client.error.VastUserException;
import com.vastdata.trino.VastTrinoDependenciesFactory;

public final class TrinoTableStatisticsIdentifierFactory
{
    private static final String VAST_STATISTICS_FILE_SUFFIX = "stats";

    private static final String tag = VastTrinoDependenciesFactory.getVastTrinoVersionedClientTag();
    private static String removeStartingSlash(String url)
    {
        if (url.startsWith("/")) {
            return url.substring(1);
        }
        else {
            return url;
        }
    }

    public static String getTableStatisticsIdentifier(String bucketName, String handleIdString) {
        return ParsedURL.PATH_SEPERATOR + String.join(ParsedURL.PATH_SEPERATOR, bucketName,
                String.join(".", handleIdString, tag, VAST_STATISTICS_FILE_SUFFIX));
    }

    public String getBucket(String tableStatisticsIdentifier) {
        try {
            return ParsedURL.of(tableStatisticsIdentifier).getBucket();
        } catch (VastUserException e) {
            throw new RuntimeException(e);
        }
    }

    public String getKeyName(String tableStatisticsIdentifier) {
        // remove the excessive '/' - https://vastdata.atlassian.net/browse/ORION-92305
        return tableStatisticsIdentifier.split("/")[2];
    }
}

