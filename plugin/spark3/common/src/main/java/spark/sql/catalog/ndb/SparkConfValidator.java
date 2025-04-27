package spark.sql.catalog.ndb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.BiFunction;

public class SparkConfValidator
{
    private static final Logger LOG = LoggerFactory.getLogger(SparkConfValidator.class);

    public final static String SETTING_SPARK_MAX_FAILURES = "spark.task.maxFailures";
    public final static String SETTING_SPARK_SPECULATION = "spark.speculation";
    public final static String SETTING_DISABLE_SPARK_DUPLICATE_WRITES_PROTECTION =
            "spark.ndb.dangerous_disable_spark_duplicate_writes_protection";
    final static String FORMAT_UNSAFE_SPARK_CONFIGURATION = String.format(
            "Unsafe Spark configuration blocks write.\nExpected:\n\t%s == 1\n\t%s == false\nGot:\n\t%s == %%d\n\t%s == %%b",
            SETTING_SPARK_MAX_FAILURES,
            SETTING_SPARK_SPECULATION,
            SETTING_SPARK_MAX_FAILURES,
            SETTING_SPARK_SPECULATION
    );

    public final int sparkTaskMaxFailures;
    public final boolean sparkSpeculation;
    public final boolean disableSparkDuplicateWritesProtection;
    public final Optional<RuntimeException> writeError;

    public SparkConfValidator(BiFunction<String, Integer, Integer> intConfSupplier, BiFunction<String, Boolean, Boolean> boolConfSupplier) {
        sparkTaskMaxFailures = intConfSupplier.apply(SETTING_SPARK_MAX_FAILURES, 4);
        LOG.debug("{} = {}", SETTING_SPARK_MAX_FAILURES, sparkTaskMaxFailures);
        sparkSpeculation = boolConfSupplier.apply(SETTING_SPARK_SPECULATION, false);
        LOG.debug("{} = {}", SETTING_SPARK_SPECULATION, sparkSpeculation);
        disableSparkDuplicateWritesProtection =
                boolConfSupplier.apply(SETTING_DISABLE_SPARK_DUPLICATE_WRITES_PROTECTION, false);
        LOG.debug("{} = {}", SETTING_DISABLE_SPARK_DUPLICATE_WRITES_PROTECTION, disableSparkDuplicateWritesProtection);
        writeError = disableSparkDuplicateWritesProtection || ((1 == sparkTaskMaxFailures) && !sparkSpeculation)
                ? Optional.empty()
                : Optional.of(new RuntimeException(String.format(FORMAT_UNSAFE_SPARK_CONFIGURATION, sparkTaskMaxFailures, sparkSpeculation)));

    }


}
