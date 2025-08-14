/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastDependenciesFactory;
import com.vastdata.client.error.VastUserException;
import com.vastdata.spark.VastSparkDependenciesFactory;
import org.apache.parquet.Strings;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.slf4j.LoggerFactory;
import scala.collection.mutable.HashMap;

import java.net.URI;
import java.util.Optional;

import static com.vastdata.client.VastConfig.DYNAMIC_FILTER_COMPACTION_THRESHOLD_DEFAULT_VALUE;
import static com.vastdata.client.VastConfig.MIN_MAX_COMPACTION_MIN_VALUES_DEFAULT_VALUE;
import static com.vastdata.client.VastConfig.SPLIT_SIZE_MULTIPLIER_DEFAULT;
import static com.vastdata.client.VastConfig.QUERY_DATA_ROWS_PER_SPLIT_DEFAULT;
import static com.vastdata.client.VastConfig.NUM_OF_SUB_SPLITS_DEFAULT;
import static com.vastdata.client.VastConfig.ROW_GROUPS_PER_SUB_SPLIT_DEFAULT;;
import static com.vastdata.client.VastConfig.TX_KEEP_ALIVE_ENABLED_DEFAULT;
import static com.vastdata.client.VastConfig.TX_KEEP_ALIVE_INTERVAL_DEFAULT;

public final class NDB
        extends NDBCommon
{
    static {
        LOG = LoggerFactory.getLogger(NDB.class);
        vastConfigSupplier = () -> {
            SparkContext sparkContext = SparkContext$.MODULE$.getActive().get();
            return getVastConfigFromSparkConf(sparkContext.getConf(), sparkContext.version());
        };
        initRoutine = NDB::init;
        dependencyFactoryFunction = VastSparkDependenciesFactory::new;
        alterTransaction = (cancelOnFailure, f) -> {
            final HashMap<String, String> environment = SparkContext$.MODULE$.getActive().get().executorEnvs();
            final Optional<String> currentTransaction = Optional.ofNullable(environment.get(TRANSACTION_KEY).getOrElse(() -> null));
            try {
                final Optional<String> newTransaction = f.apply(currentTransaction);
                if (newTransaction.isPresent()) {
                    environment.put(TRANSACTION_KEY, newTransaction.get());
                } else {
                    environment.remove(TRANSACTION_KEY);
                }
            }
            catch (final Exception error) {
                if (cancelOnFailure) {
                    environment.remove(TRANSACTION_KEY);
                }
                throw error;
            }
        };
    }

    private NDB() {}

    public static synchronized void init(VastConfig vastConfig)
    {
        NDBCommon.initCommonConfig(vastConfig);
    }

    public static synchronized VastConfig getConfig()
            throws VastUserException
    {
        return NDBCommon.getConfig();
    }

    public static void clearConfig()
    {
        NDBCommon.clearConfig();
    }

    public static void init()
    {
        NDBCommon.init();
    }

    public static VastClient getVastClient(VastConfig vastConfig)
            throws VastUserException
    {
        return NDBCommon.getVastClient(vastConfig);
    }

    public static VastDependenciesFactory getSparkDependenciesFactory()
    {
        return NDBCommon.getSparkDependenciesFactory();
    }

    private static VastConfig getVastConfigFromSparkConf(SparkConf conf, String engineVersion)
    {
        printConf(conf);
        String vastEndpoint = conf.get("spark.ndb.endpoint");
        logConfEntry("spark.ndb.endpoint", vastEndpoint);
        String endPointsList = conf.get("spark.ndb.data_endpoints", vastEndpoint);
        logConfEntry("spark.ndb.data_endpoints", endPointsList);
        if (Strings.isNullOrEmpty(endPointsList)) {
            logConfEntry("spark.ndb.data_endpoints", vastEndpoint);
            endPointsList = vastEndpoint;
        }
        String accessKeyId = conf.get("spark.ndb.access_key_id");
        logConfEntry("spark.ndb.access_key_id", accessKeyId);
        String secretAccessKey = conf.get("spark.ndb.secret_access_key");
        int numOfSplits = conf.getInt("spark.ndb.num_of_splits", 256);
        int numOfSubSplits = conf.getInt("spark.ndb.num_of_sub_splits", NUM_OF_SUB_SPLITS_DEFAULT);
        int rowGroupsPerSubSplit = conf.getInt("spark.ndb.rowgroups_per_subsplit", ROW_GROUPS_PER_SUB_SPLIT_DEFAULT);
        int queryDataRowsPerPage = conf.getInt("spark.ndb.query_data_rows_per_page", 100000);
        long queryDataRowsPerSplit = conf.getLong("spark.ndb.query_data_rows_per_split", QUERY_DATA_ROWS_PER_SPLIT_DEFAULT);
        int maxInsertRows = conf.getInt("spark.ndb.max_row_count_per_insert", 16000);
        int maxUpdateRows = conf.getInt("spark.ndb.max_row_count_per_update", 2048);
        int maxDeleteRows = conf.getInt("spark.ndb.max_row_count_per_delete", 2048);
        int retriesMaxCount = conf.getInt("spark.ndb.retry_max_count", 3);
        int retrySleepDuration = conf.getInt("spark.ndb.retry_sleep_duration", 1000);
        boolean parallelImport = conf.getBoolean("spark.ndb.parallel_import", true);
        boolean enableSortedProjections = conf.getBoolean("spark.ndb.enable_sorted_projections", false);
        int minMaxCompactionMinValuesThreshold = conf.getInt("spark.ndb.min_max_compaction_min_values_threshold", MIN_MAX_COMPACTION_MIN_VALUES_DEFAULT_VALUE);
        int dynamicFilterCompactionThreshold = conf.getInt("spark.ndb.dynamic_filter_compaction_threshold", DYNAMIC_FILTER_COMPACTION_THRESHOLD_DEFAULT_VALUE);
        int dynamicFilterMaxValuesThreshold = conf.getInt("spark.ndb.dynamic_filter_max_values_threshold", 1000);
        int dynamicFilterWaitTimeout = conf.getInt("spark.ndb.dynamic_filtering_wait_timeout", 2 * 1000);
        int keepAliveInterval = conf.getInt("spark.ndb.vast_transaction_keep_alive_interval_seconds", TX_KEEP_ALIVE_INTERVAL_DEFAULT);
        boolean keepAliveEnabled = conf.getBoolean("spark.ndb.vast_transaction_keep_alive_enabled", TX_KEEP_ALIVE_ENABLED_DEFAULT);
        int splitSizeMultiplier = conf.getInt("spark.ndb.split_size_multiplier", SPLIT_SIZE_MULTIPLIER_DEFAULT);
        return new VastConfig()
                .setEndpoint(URI.create(vastEndpoint))
                .setDataEndpoints(endPointsList)
                .setRegion("vast")
                .setAccessKeyId(accessKeyId)
                .setSecretAccessKey(secretAccessKey)
                .setNumOfSplits(numOfSplits)
                .setNumOfSubSplits(numOfSubSplits)
                .setRowGroupsPerSubSplit(rowGroupsPerSubSplit)
                .setQueryDataRowsPerPage(queryDataRowsPerPage)
                .setQueryDataRowsPerSplit(queryDataRowsPerSplit)
                .setMaxRowsPerInsert(maxInsertRows)
                .setMaxRowsPerUpdate(maxUpdateRows)
                .setMaxRowsPerDelete(maxDeleteRows)
                .setRetryMaxCount(retriesMaxCount)
                .setRetrySleepDuration(retrySleepDuration)
                .setParallelImport(parallelImport)
                .setEnableSortedProjections(enableSortedProjections)
                .setMinMaxCompactionMinValuesThreshold(minMaxCompactionMinValuesThreshold)
                .setDynamicFilterCompactionThreshold(dynamicFilterCompactionThreshold)
                .setDynamicFilterMaxValuesThreshold(dynamicFilterMaxValuesThreshold)
                .setDynamicFilteringWaitTimeout(dynamicFilterWaitTimeout)
                .setVastTransactionKeepAliveEnabled(keepAliveEnabled)
                .setVastTransactionKeepAliveIntervalSeconds(keepAliveInterval)
                .setSplitSizeMultiplier(splitSizeMultiplier)
                .setEngineVersion(engineVersion);
    }

    private static void printConf(SparkConf conf)
    {
        LOG.debug("Initializing using Spark conf: {}, {}", conf, conf.getAll());
    }
}
