/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastUserException;
import com.vastdata.spark.VastSparkDependenciesFactory;
import com.vastdata.spark.error.SparkExceptionFactory;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.apache.parquet.Strings;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.vastdata.client.VastConfig.DYNAMIC_FILTER_COMPACTION_THRESHOLD_DEFAULT_VALUE;
import static com.vastdata.client.VastConfig.MIN_MAX_COMPACTION_MIN_VALUES_DEFAULT_VALUE;
import static com.vastdata.client.VastConfig.TX_KEEP_ALIVE_ENABLED_DEFAULT;
import static com.vastdata.client.VastConfig.TX_KEEP_ALIVE_INTERVAL_DEFAULT;

public final class NDB
{
    private static final Logger LOG = LoggerFactory.getLogger(NDB.class);

    private static final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private static VastConfig config = null;
    private static HttpClient httpClient = null;
    private static VastClient vastClient = null;
    private static VastSparkDependenciesFactory sparkDependenciesFactory;

    private NDB() {}

    // called using ndb.NDB.init(spark)
    public static void init()
    {
        if (!isInitialized.get()) {
//            try {
//                LOG.debug("Trying to override spark table-scan classes");
//                overrideV2Relation();
//            }
//            catch (NotFoundException | CannotCompileException e) {
//                throw new RuntimeException("Override spark table-scan classes failed during NDB initialization", e);
//            }
            SparkContext sparkContext = SparkContext$.MODULE$.getActive().get();
            init(getVastConfigFromSparkConf(sparkContext.getConf(), sparkContext.version()));
        }
        else {
            LOG.warn("Module already initialized");
        }
    }

    public static synchronized void init(VastConfig vastConfig)
    {
        if (!isInitialized.get()) {
            HttpClientConfig httpConfig = new HttpClientConfig();
            sparkDependenciesFactory = new VastSparkDependenciesFactory(vastConfig);
            sparkDependenciesFactory.getHttpClientConfigConfigDefaults().setDefaults(httpConfig);
            HttpClient tmpHttpClient = new JettyHttpClient("ndb", httpConfig);
            config = vastConfig;
            httpClient = tmpHttpClient;
            isInitialized.set(true);
        }
        else {
            LOG.warn("Module already configured");
        }
    }

    public static void clearConfig()
    {
        isInitialized.set(false);
        config = null;
        httpClient = null;
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
        logConfEntry("spark.ndb.secret_access_key", secretAccessKey);
        int numOfSplits = conf.getInt("spark.ndb.num_of_splits", 256);
        int numOfSubSplits = conf.getInt("spark.ndb.num_of_sub_splits", 20);
        int rowGroupsPerSubSplit = conf.getInt("spark.ndb.rowgroups_per_subsplit", 1);
        int queryDataRowsPerPage = conf.getInt("spark.ndb.query_data_rows_per_page", 100000);
        int queryDataRowsPerSplit = conf.getInt("spark.ndb.query_data_rows_per_split", 4000000);
        int retriesMaxCount = conf.getInt("spark.ndb.retry_max_count", 3);
        int retrySleepDuration = conf.getInt("spark.ndb.retry_sleep_duration", 1000);
        boolean parallelImport = conf.getBoolean("spark.ndb.parallel_import", true);
        int minMaxCompactionMinValuesThreshold = conf.getInt("spark.ndb.min_max_compaction_min_values_threshold", MIN_MAX_COMPACTION_MIN_VALUES_DEFAULT_VALUE);
        int dynamicFilterCompactionThreshold = conf.getInt("spark.ndb.dynamic_filter_compaction_threshold", DYNAMIC_FILTER_COMPACTION_THRESHOLD_DEFAULT_VALUE);
        int dynamicFilterMaxValuesThreshold = conf.getInt("spark.ndb.dynamic_filter_max_values_threshold", 1000);
        int dynamicFilterWaitTimeout = conf.getInt("spark.ndb.dynamic_filtering_wait_timeout", 2 * 1000);
        int keepAliveInterval = conf.getInt("spark.ndb.vast_transaction_keep_alive_interval_seconds", TX_KEEP_ALIVE_INTERVAL_DEFAULT);
        boolean keepAliveEnabled = conf.getBoolean("spark.ndb.vast_transaction_keep_alive_enabled", TX_KEEP_ALIVE_ENABLED_DEFAULT);
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
                .setRetryMaxCount(retriesMaxCount)
                .setRetrySleepDuration(retrySleepDuration)
                .setParallelImport(parallelImport)
                .setMinMaxCompactionMinValuesThreshold(minMaxCompactionMinValuesThreshold)
                .setDynamicFilterCompactionThreshold(dynamicFilterCompactionThreshold)
                .setDynamicFilterMaxValuesThreshold(dynamicFilterMaxValuesThreshold)
                .setDynamicFilteringWaitTimeout(dynamicFilterWaitTimeout)
                .setVastTransactionKeepAliveEnabled(keepAliveEnabled)
                .setVastTransactionKeepAliveIntervalSeconds(keepAliveInterval)
                .setEngineVersion(engineVersion);
    }

    private static void logConfEntry(String key, String vastEndpoint)
    {
        LOG.info("NDB init conf: {}, {}", key, vastEndpoint);
    }

    public static synchronized VastConfig getConfig()
            throws VastUserException
    {
        if (config == null) {
            init();
            if (config == null) {
                throw SparkExceptionFactory.uninitializedConfig();
            }
            else {
                return config;
            }
        }
        else {
            return config;
        }
    }

    public static synchronized HttpClient getHTTPClient()
            throws VastUserException
    {
        if (httpClient == null) {
            init();
            if (httpClient == null) {
                throw SparkExceptionFactory.uninitializedConfig();
            }
            else {
                return httpClient;
            }
        }
        else {
            return httpClient;
        }
    }

    private static void printConf(SparkConf conf)
    {
        LOG.debug("Initializing using Spark conf: {}, {}", conf, conf.getAll());
    }

    public static VastClient getVastClient(VastConfig vastConfig)
            throws VastUserException
    {
        if (vastClient == null) {
            createVastClient(vastConfig);
        }
        return vastClient;
    }

    private static synchronized void createVastClient(VastConfig vastConfig)
            throws VastUserException
    {
        if (vastClient == null) {
            init(vastConfig);
            vastClient = new VastClient(getHTTPClient(), vastConfig, new VastSparkDependenciesFactory(vastConfig));
        }
    }

    public static VastSparkDependenciesFactory getSparkDependenciesFactory()
    {
        if (sparkDependenciesFactory == null) {
            return new VastSparkDependenciesFactory(null);
        }
        else {
            return sparkDependenciesFactory;
        }
    }
}
