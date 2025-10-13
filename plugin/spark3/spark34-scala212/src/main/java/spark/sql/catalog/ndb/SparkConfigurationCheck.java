/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class SparkConfigurationCheck {
    private static final Logger LOG = LoggerFactory.getLogger(SparkConfigurationCheck.class);

    public final static String SETTING_SPARK_MAX_FAILURES = "spark.task.maxFailures";
    public final static String SETTING_SPARK_SPECULATION = "spark.speculation";
    public final static String SETTING_DISABLE_SPARK_DUPLICATE_WRITES_PROTECTION =
            "spark.ndb.dangerous_disable_spark_duplicate_writes_protection";
    public final static String FORMAT_UNSAFE_SPARK_CONFIGURATION = String.format(
            "Unsafe Spark configuration blocks write.\nExpected:\n\t%s == 1\n\t%s == false\nGot:\n\t%s == %%d\n\t%s == %%b",
            SETTING_SPARK_MAX_FAILURES,
            SETTING_SPARK_SPECULATION,
            SETTING_SPARK_MAX_FAILURES,
            SETTING_SPARK_SPECULATION
    );

    public final SparkConf sparkConfiguration;
    public final int sparkTaskMaxFailures;
    public final boolean sparkSpeculation;
    public final boolean disableSparkDuplicateWritesProtection;
    public final Optional<RuntimeException> writeError;

    public SparkConfigurationCheck(final SparkConf sparkConfiguration)
    {
        this.sparkConfiguration = sparkConfiguration;
        sparkTaskMaxFailures = sparkConfiguration.getInt(SETTING_SPARK_MAX_FAILURES, 4);
        LOG.debug("{} = {}", SETTING_SPARK_MAX_FAILURES, sparkTaskMaxFailures);
        sparkSpeculation = sparkConfiguration.getBoolean(SETTING_SPARK_SPECULATION, false);
        LOG.debug("{} = {}", SETTING_SPARK_SPECULATION, sparkSpeculation);
        disableSparkDuplicateWritesProtection =
                sparkConfiguration.getBoolean(SETTING_DISABLE_SPARK_DUPLICATE_WRITES_PROTECTION, false);
        LOG.debug("{} = {}", SETTING_DISABLE_SPARK_DUPLICATE_WRITES_PROTECTION, disableSparkDuplicateWritesProtection);
        writeError = disableSparkDuplicateWritesProtection || ((1 == sparkTaskMaxFailures) && !sparkSpeculation)
                ? Optional.empty()
                : Optional.of(new RuntimeException(String.format(FORMAT_UNSAFE_SPARK_CONFIGURATION, sparkTaskMaxFailures, sparkSpeculation)));
    }
}
