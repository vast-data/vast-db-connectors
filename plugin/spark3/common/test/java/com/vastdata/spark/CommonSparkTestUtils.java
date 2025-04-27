package com.vastdata.spark;

import java.util.HashMap;
import java.util.Map;

public class CommonSparkTestUtils
{
    private CommonSparkTestUtils() {}

    public static final Map<String, Object> DEFAULT_SPARK_CONF = new HashMap<>();

    static {
        DEFAULT_SPARK_CONF.put("spark.sql.catalog.ndb", "spark.sql.catalog.ndb.VastCatalog");
        DEFAULT_SPARK_CONF.put("spark.sql.defaultCatalog", "ndb");
        DEFAULT_SPARK_CONF.put("spark.sql.extensions", "ndb.NDBSparkSessionExtension");
        DEFAULT_SPARK_CONF.put("spark.sql.readSideCharPadding", false);
        DEFAULT_SPARK_CONF.put("spark.sql.optimizer.runtimeFilter.semiJoinReduction.enabled", "true");
        DEFAULT_SPARK_CONF.put("spark.driver.extraClassPath", "src/tabular/trino/plugin/spark3/resources/*");
        DEFAULT_SPARK_CONF.put("spark.driver.userClassPathFirst", true);
        DEFAULT_SPARK_CONF.put("spark.driver.bindAddress", "127.0.0.1");
        DEFAULT_SPARK_CONF.put("spark.ndb.access_key_id", "pIX3SzyuQVmdrIVZnyy0");
        DEFAULT_SPARK_CONF.put("spark.ndb.secret_access_key", "5c5HqW3cDQsUNg68OlhJmq72TM2nZxcP5lR6D1ps");
        DEFAULT_SPARK_CONF.put("spark.ndb.num_of_splits", "2");
        DEFAULT_SPARK_CONF.put("spark.ndb.vast_transaction_keep_alive_interval_seconds", 1);
    }
}
