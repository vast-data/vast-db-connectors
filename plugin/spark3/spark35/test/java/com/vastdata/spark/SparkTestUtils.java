/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.VastConfig;
import ndb.NDB;
import org.apache.spark.sql.SparkSession;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static com.vastdata.spark.CommonSparkTestUtils.DEFAULT_SPARK_CONF;
import static java.lang.String.format;
import static spark.sql.catalog.ndb.SparkConfValidator.SETTING_DISABLE_SPARK_DUPLICATE_WRITES_PROTECTION;
import static spark.sql.catalog.ndb.SparkConfValidator.SETTING_SPARK_MAX_FAILURES;
import static spark.sql.catalog.ndb.SparkConfValidator.SETTING_SPARK_SPECULATION;

public final class SparkTestUtils
{
    private SparkTestUtils() {}

    private static Map<String, Object> getFullConf(Map<String, Object> extraConfs)
    {
        HashMap<String, Object> tmp = new HashMap<>(DEFAULT_SPARK_CONF);
        tmp.putAll(extraConfs);
        return tmp;
    }

    public static SparkSession getSession()
    {
        return getSession(80);
    }

    public static SparkSession getSession(final int testPort)
    {
        return getSession(testPort, 1, false, false);
    }

    public static SparkSession getSession(final int testPort, Map<String, Object> extraConfs)
    {
        return getSession(testPort, 1, false, false, extraConfs);
    }

    public static SparkSession getSession(final int testPort, final int maxFailures, final boolean speculation,
                                          final boolean disableSparkDuplicateWritesProtection)
    {
        return getSession(testPort, maxFailures, speculation, disableSparkDuplicateWritesProtection, new HashMap<>());
    }

    private static SparkSession getSession(int testPort, int maxFailures, boolean speculation,
            boolean disableSparkDuplicateWritesProtection, Map<String, Object> tmp)
    {
        // force the new configuration to replace the old one
        NDB.clearConfig();
        SparkSession.Builder builder = SparkSession.builder();
        tmp.put(SETTING_SPARK_MAX_FAILURES, maxFailures);
        tmp.put(SETTING_SPARK_SPECULATION, speculation);
        tmp.put(SETTING_DISABLE_SPARK_DUPLICATE_WRITES_PROTECTION, disableSparkDuplicateWritesProtection);
        tmp.put("spark.ndb.endpoint", getEndpointString(testPort));
        Map<String, Object> fullConf = getFullConf(tmp);
        fullConf.forEach((key, value) -> builder.config(key, value.toString()));
        return builder.appName("example").master("local[*]").getOrCreate();
    }

    private static String getEndpointString(int testPort)
    {
        return format("http://localhost:%d", testPort);
    }

    public static VastConfig getTestConfig()
    {
        return new VastConfig()
                .setEndpoint(URI.create(getEndpointString(1234)))
                .setAccessKeyId("pIX3SzyuQVmdrIVZnyy0")
                .setSecretAccessKey("5c5HqW3cDQsUNg68OlhJmq72TM2nZxcP5lR6D1ps")
                .setRegion("us-east-1");
    }
}
