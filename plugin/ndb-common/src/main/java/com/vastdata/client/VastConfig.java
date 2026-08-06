/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.vastdata.client.importdata.EvenSizeWithLimitChunkifier;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.MINUTES;

public class VastConfig
        implements Serializable
{
    public static final int MIN_SUB_SPLITS = 1;
    public static final int MAX_SUB_SPLITS = 64;
    public static final String DYNAMIC_FILTER_COMPACTION_THRESHOLD = "dynamic_filter_compaction_threshold";
    public static final String DYNAMIC_FILTER_PUSHDOWN_THRESHOLD = "dynamic_filter_pushdown_threshold";
    public static final String MIN_MAX_COMPACTION_MIN_VALUES_THRESHOLD = "min_max_compaction_min_values_threshold";
    public static final int MIN_MAX_COMPACTION_MIN_VALUES_DEFAULT_VALUE = 15;
    public static final int DYNAMIC_FILTER_COMPACTION_THRESHOLD_DEFAULT_VALUE = 1000;
    public static final boolean TX_KEEP_ALIVE_ENABLED_DEFAULT = true;
    public static final int TX_KEEP_ALIVE_INTERVAL_DEFAULT = 60;
    public static final int SPLIT_SIZE_MULTIPLIER_DEFAULT = 3;
    public static final int NUM_OF_SUB_SPLITS_DEFAULT = 20;
    public static final int ROW_GROUPS_PER_SUB_SPLIT_DEFAULT = 8;
    public static final int DEFAULT_QUERY_DATA_ROWS_PER_PAGE = 128 * 1024; // should be a multiple of a row group size (2 ** 16 rows)
    public static final int DEFAULT_RETRY_MAX_COUNT = 600;
    public static final int DEFAULT_DYNAMIC_FILTER_MAX_VALUES_THRESHOLD = 10000;
    public static final int DEFAULT_MAX_ROWS_PER_INSERT = (int) Math.pow(2, 19);
    // queryDataRowsPerSplit must be a multiple of (2^16 * numOfSubSplits * rowGroupsPerSubSplit)
    public static final long QUERY_DATA_ROWS_PER_SPLIT_DEFAULT = (long) Math.pow(
            2,
            16) * NUM_OF_SUB_SPLITS_DEFAULT * ROW_GROUPS_PER_SUB_SPLIT_DEFAULT;
    public static final String ENABLE_END_USER_IMPERSONATION = "enable_end_user_impersonation";
    public static final String ENABLE_ROW_COLUMN_SECURITY = "enable_row_column_security";
    public static final String ENABLE_ACCESS_CONTROL = "enable_access_control";
    private static final Logger LOG = Logger.get(VastConfig.class);
    private static final long KB = 1024L;
    private static final long MB = 1024L * KB;
    private static final Splitter SPLITTER = Splitter
            .on(',')
            .trimResults()
            .omitEmptyStrings();
    private final int maxColumnSize = 128 * 1024;
    private URI endpoint = URI.create("http://localhost:9090");
    private List<URI> dataEndpoints; // optional endpoints for data-related queries
    private String accessKeyId;
    private String secretAccessKey;
    private String region = "vast";
    private boolean enableCustomSchemaSeparator;
    private String customSchemaSeparator = "|";
    private int numOfSplits = 1;
    private int numOfSubSplits = NUM_OF_SUB_SPLITS_DEFAULT;
    private int rowGroupsPerSubSplit = ROW_GROUPS_PER_SUB_SPLIT_DEFAULT;
    private long queryDataRowsPerSplit = QUERY_DATA_ROWS_PER_SPLIT_DEFAULT;
    private int queryDataRowsPerPage = DEFAULT_QUERY_DATA_ROWS_PER_PAGE; // should be a multiple of a row group size (2 ** 16 rows)
    private int queryDataRowsPerBatch = 128 * 1024; // should be a multiple of a row group size (2 ** 16 rows)
    private long maxRequestBodySize = 5 * MB; // must be <= Mooktze largest buffer size (see `src/plasma/execution/silo.cpp`)
    private int insertBufferTargetRowCountPerPartitionFlush = (int) Math.pow(2,
            16);

    private long bufferingBufferSizeSoftLimitInBytes = DataSize
            .valueOf("100MB")
            .toBytes();

    private int bufferingBufferOpenVsrTargetRowCount = (int) Math.pow(2, 12);
    private int bufferingBufferOpenVsrRowCountPreallocation = (int) Math.pow(2,
            12);

    private long advisoryPartitionSize = 256 * MB;
    private boolean adaptivePartitioning = true;
    private int splitSizeMultiplier = SPLIT_SIZE_MULTIPLIER_DEFAULT;

    private int retryMaxCount = DEFAULT_RETRY_MAX_COUNT; // 10 minutes of retries in case we can't connect to VAST
    private int retrySleepDuration = 1000;
    private boolean parallelImport = true;
    private int dynamicFilteringWaitTimeout = 2 * 1000;
    private int dynamicFilteringWaitTimeoutFactor = 2;

    private int dynamicFilterCompactionThreshold = 1000;
    private int dynamicFilterPushdownThreshold = 99;
    private int dynamicFilterElysiumCompactionMultiplier = 10;
    private int dynamicFilterMaxValuesThreshold = DEFAULT_DYNAMIC_FILTER_MAX_VALUES_THRESHOLD;
    private int minMaxCompactionMinValuesThreshold = 15;

    private String engineVersion = "NA";

    private boolean enablePredicatePushdown = true;
    private boolean matchSubstringPushdown = true;
    private boolean complexPredicatePushdown;
    private boolean expressionProjectionPushdown;
    private boolean enableSortedProjections = true;
    private boolean reportPartitioning; // Spark only
    private boolean onlyOrderedPushdown;

    private int maxRowCountPerInsert = DEFAULT_MAX_ROWS_PER_INSERT;
    private int maxRowCountPerUpdate = 64 * 1024;
    private int maxRowCountPerDelete = 64 * 1024;

    private int importChunkLimit = EvenSizeWithLimitChunkifier.CHUNK_SIZE_LIMIT;

    private long maxStatisticsFilesSupportedPerSession = 10000;
    private boolean keepFilterAfterPushdown = true;
    private boolean vastTransactionKeepAliveEnabled = TX_KEEP_ALIVE_ENABLED_DEFAULT;
    private int vastTransactionKeepAliveIntervalSeconds = TX_KEEP_ALIVE_INTERVAL_DEFAULT;
    private boolean estimateSplitsFromRowIdPredicate;
    private boolean estimateSplitsFromElysium = true;
    private int minRowsForPartitionSplitEstimation = 1000000;

    private Long seedForShufflingEndpoints;
    private boolean useColumnHistogram = true; // Relevant for spark only

    private int compression;
    private int compressionMinSavings = 30;
    private int compressionLevel = 1;

    private boolean enableAccessControl;
    private boolean enableRowColumnSecurity;
    private boolean enableEndUserImpersonation;
    private boolean partitionedInsert = true;

    private int maxInsertBuckets = 1000;

    private boolean enableZeroRowsOptimization;
    private boolean enablePrefillOptimization;
    private boolean enableServerStatsCollection;

    private long insertPartitionSize = 4 * 1024 * 1024;
    private boolean insertExactPartitioning = true;

    private int shapingLoggerThreshold;
    private Duration shapingLoggerDuration = Duration.of(1, MINUTES);
    private int shapingLoggerNumberOfSamples = 3;

    private int nodeIoExecutorNumThreads = 64;
    private int bufferedInserterMaxWritePermits = 64;
    private int bufferedInserterMaxJobPermits = 32;

    private long memoryLimiterMaxAllowed = DataSize.valueOf("100TB").toBytes(); // using 100TB which is actually limitless
    private Duration memoryLimiterHangingValidationInterval = Duration.of(1, MINUTES);
    private Duration memoryLimiterHangingReleasePeriod = Duration.of(1, MINUTES);
    private int memoryLimitMaxNumRunnerFactor = 5;
    private boolean enableMemoryLimit = true;

    private Duration metricDumperInterval = Duration.of(15, MINUTES);

    public VastConfig()
    {
    }

    public URI getEndpoint()
    {
        return endpoint;
    }

    @Config("endpoint")
    public VastConfig setEndpoint(URI endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public List<URI> getDataEndpoints()
    {
        return Optional
                .ofNullable(dataEndpoints)
                .orElse(ImmutableList.of(endpoint));
    }

    @Config("data_endpoints")
    public VastConfig setDataEndpoints(String dataEndpoints)
    {
        this.dataEndpoints = SPLITTER
                .splitToStream(dataEndpoints)
                .map(URI::create)
                .collect(Collectors.toList());
        return this;
    }

    public boolean getEnableCustomSchemaSeparator()
    {
        return enableCustomSchemaSeparator;
    }

    @Config("enable_custom_schema_separator")
    public VastConfig setEnableCustomSchemaSeparator(
            boolean enableCustomSchemaSeparator)
    {
        this.enableCustomSchemaSeparator = enableCustomSchemaSeparator;
        return this;
    }

    @NotEmpty
    public String getCustomSchemaSeparator()
    {
        return customSchemaSeparator;
    }

    @Config("custom_schema_separator")
    public VastConfig setCustomSchemaSeparator(String customSchemaSeparator)
    {
        this.customSchemaSeparator = customSchemaSeparator;
        return this;
    }

    @NotNull
    public String getAccessKeyId()
    {
        return accessKeyId;
    }

    @Config("access_key_id")
    public VastConfig setAccessKeyId(String accessKeyId)
    {
        this.accessKeyId = accessKeyId;
        return this;
    }

    @NotNull
    public String getSecretAccessKey()
    {
        return secretAccessKey;
    }

    @Config("secret_access_key")
    @ConfigSecuritySensitive
    public VastConfig setSecretAccessKey(String secretAccessKey)
    {
        this.secretAccessKey = secretAccessKey;
        return this;
    }

    public String getRegion()
    {
        return region;
    }

    @Config("region")
    public VastConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }

    @Min(1)
    public int getNumOfSplits()
    {
        return numOfSplits;
    }

    @Config("num_of_splits")
    public VastConfig setNumOfSplits(int numOfSplits)
    {
        this.numOfSplits = numOfSplits;
        return this;
    }

    @Min(MIN_SUB_SPLITS)
    @Max(MAX_SUB_SPLITS)
    public int getNumOfSubSplits()
    {
        return numOfSubSplits;
    }

    @Config("num_of_subsplits")
    public VastConfig setNumOfSubSplits(int numOfSubSplits)
    {
        this.numOfSubSplits = numOfSubSplits;
        return this;
    }

    @Min(1)
    public int getRowGroupsPerSubSplit()
    {
        return rowGroupsPerSubSplit;
    }

    @Config("rowgroups_per_subsplit")
    public VastConfig setRowGroupsPerSubSplit(int rowGroupsPerSubSplit)
    {
        this.rowGroupsPerSubSplit = rowGroupsPerSubSplit;
        return this;
    }

    public long getQueryDataRowsPerSplit()
    {
        return queryDataRowsPerSplit;
    }

    @Config("query_data_rows_per_split")
    public VastConfig setQueryDataRowsPerSplit(long queryDataRowsPerSplit)
    {
        this.queryDataRowsPerSplit = queryDataRowsPerSplit;
        return this;
    }

    public int getQueryDataRowsPerPage()
    {
        return queryDataRowsPerPage;
    }

    @Config("query_data_rows_per_page")
    public VastConfig setQueryDataRowsPerPage(int queryDataRowsPerPage)
    {
        this.queryDataRowsPerPage = queryDataRowsPerPage;
        return this;
    }

    public long getMaxRequestBodySize()
    {
        return maxRequestBodySize;
    }

    @Config("max_request_body_size")
    public VastConfig setMaxRequestBodySize(long maxRequestBodySize)
    {
        this.maxRequestBodySize = maxRequestBodySize;
        return this;
    }

    public DataSize getBufferingBufferSizeSoftLimit()
    {
        return DataSize.succinctBytes(bufferingBufferSizeSoftLimitInBytes);
    }

    @Config("write-buffering.soft-limit")
    public VastConfig setBufferingBufferSizeSoftLimit(DataSize value)
    {
        this.bufferingBufferSizeSoftLimitInBytes = value.toBytes();
        return this;
    }

    public long getBufferingBufferSizeSoftLimitInBytes()
    {
        return bufferingBufferSizeSoftLimitInBytes;
    }

    public int getBufferingBufferOpenVsrTargetRowCount()
    {
        return bufferingBufferOpenVsrTargetRowCount;
    }

    @Config("write-buffering.open-vsr-target-row-count")
    public VastConfig setBufferingBufferOpenVsrTargetRowCount(int value)
    {
        this.bufferingBufferOpenVsrTargetRowCount = value;
        return this;
    }

    public int getBufferingBufferOpenVsrRowCountPreallocation()
    {
        return bufferingBufferOpenVsrRowCountPreallocation;
    }

    @Config("write-buffering.open-vsr-row-count-preallocation")
    public VastConfig setBufferingBufferOpenVsrRowCountPreallocation(int value)
    {
        this.bufferingBufferOpenVsrRowCountPreallocation = value;
        return this;
    }

    public int getInsertBufferTargetRowCountPerPartitionFlush()
    {
        return insertBufferTargetRowCountPerPartitionFlush;
    }

    @Config("write-buffering.partition-flush-target-row-count")
    public VastConfig setInsertBufferTargetRowCountPerPartitionFlush(int value)
    {
        this.insertBufferTargetRowCountPerPartitionFlush = value;
        return this;
    }

    public long getMaxColumnSize()
    {
        return maxColumnSize;
    }

    public long getAdvisoryPartitionSize()
    {
        return this.adaptivePartitioning ? this.advisoryPartitionSize : -1;
    }

    @Config("advisory_partition_size")
    public VastConfig setAdvisoryPartitionSize(long advisoryPartitionSize)
    {
        this.advisoryPartitionSize = advisoryPartitionSize;
        return this;
    }

    public boolean getAdaptivePartitioning()
    {
        return this.adaptivePartitioning;
    }

    @Config("adaptive_partitioning")
    public VastConfig setAdaptivePartitioning(boolean adaptivePartitioning)
    {
        this.adaptivePartitioning = adaptivePartitioning;
        return this;
    }

    public int getSplitSizeMultiplier()
    {
        return this.adaptivePartitioning ? this.splitSizeMultiplier : 1;
    }

    @Config("split_size_multiplier")
    public VastConfig setSplitSizeMultiplier(int splitSizeMultiplier)
    {
        this.splitSizeMultiplier = Math.max(splitSizeMultiplier, 1);
        return this;
    }

    @Min(0)
    public int getRetryMaxCount()
    {
        return retryMaxCount;
    }

    @Config("retry_max_count")
    public VastConfig setRetryMaxCount(int retryMaxCount)
    {
        this.retryMaxCount = retryMaxCount;
        return this;
    }

    @Min(0)
    public int getRetrySleepDuration()
    {
        return retrySleepDuration;
    }

    @Config("retry_sleep_duration")
    public VastConfig setRetrySleepDuration(int retrySleepDuration)
    {
        this.retrySleepDuration = retrySleepDuration;
        return this;
    }

    public boolean getParallelImport()
    {
        return parallelImport;
    }

    @Config("parallel_import")
    public VastConfig setParallelImport(boolean parallelImport)
    {
        this.parallelImport = parallelImport;
        return this;
    }

    @NotNull
    public int getDynamicFilteringWaitTimeout()
    {
        return dynamicFilteringWaitTimeout;
    }

    @Config("dynamic_filtering_wait_timeout")
    @ConfigDescription(
            "Duration to wait for completion of dynamic filters during split generation")
    public VastConfig setDynamicFilteringWaitTimeout(
            int dynamicFilteringWaitTimeout)
    {
        this.dynamicFilteringWaitTimeout = dynamicFilteringWaitTimeout;
        return this;
    }

    @NotNull
    @ConfigDescription(
            "Factor to multiply dynamic filtering wait timeout per sorted / partition column")
    public int getDynamicFilteringWaitTimeoutFactor()
    {
        return dynamicFilteringWaitTimeoutFactor;
    }

    @Config("dynamic_filtering_wait_timeout_factor")
    public VastConfig setDynamicFilteringWaitTimeoutFactor(
            int dynamicFilteringWaitTimeoutFactor)
    {
        this.dynamicFilteringWaitTimeoutFactor = dynamicFilteringWaitTimeoutFactor;
        return this;
    }

    public int getDynamicFilterCompactionThreshold()
    {
        return dynamicFilterCompactionThreshold;
    }

    @Config(DYNAMIC_FILTER_COMPACTION_THRESHOLD)
    public VastConfig setDynamicFilterCompactionThreshold(
            int dynamicFilterCompactionThreshold)
    {
        this.dynamicFilterCompactionThreshold = dynamicFilterCompactionThreshold;
        return this;
    }

    public int getDynamicFilterElysiumCompactionMultiplier()
    {
        return dynamicFilterElysiumCompactionMultiplier;
    }

    @Config("dynamic_filter_elysium_compaction_multiplier")
    public VastConfig setDynamicFilterElysiumCompactionMultiplier(
            int dynamicFilterElysiumCompactionMultiplier)
    {
        this.dynamicFilterElysiumCompactionMultiplier =
                dynamicFilterElysiumCompactionMultiplier > 0 ?
                        dynamicFilterElysiumCompactionMultiplier :
                        0;
        return this;
    }

    public int getDynamicFilterPushdownThreshold()
    {
        return dynamicFilterPushdownThreshold;
    }

    @Config(DYNAMIC_FILTER_PUSHDOWN_THRESHOLD)
    public VastConfig setDynamicFilterPushdownThreshold(
            int dynamicFilterPushdownThreshold)
    {
        this.dynamicFilterPushdownThreshold = dynamicFilterPushdownThreshold;
        return this;
    }

    public int getMinMaxCompactionMinValuesThreshold()
    {
        return minMaxCompactionMinValuesThreshold;
    }

    @Config(MIN_MAX_COMPACTION_MIN_VALUES_THRESHOLD)
    public VastConfig setMinMaxCompactionMinValuesThreshold(
            int minMaxCompactionMinValuesThreshold)
    {
        this.minMaxCompactionMinValuesThreshold = minMaxCompactionMinValuesThreshold;
        return this;
    }

    public int getDynamicFilterMaxValuesThreshold()
    {
        return this.dynamicFilterMaxValuesThreshold;
    }

    @Config("dynamic_filter_max_values_threshold")
    public VastConfig setDynamicFilterMaxValuesThreshold(
            int dynamicFilterMaxValuesThreshold)
    {
        this.dynamicFilterMaxValuesThreshold = dynamicFilterMaxValuesThreshold;
        return this;
    }

    public String getEngineVersion()
    {
        return engineVersion;
    }

    @Config("engine_version")
    public VastConfig setEngineVersion(String engineVersion)
    {
        this.engineVersion = engineVersion;
        return this;
    }

    public boolean isPredicatePushdownEnabled()
    {
        return enablePredicatePushdown;
    }

    @Config("enable_predicate_pushdown")
    public VastConfig setPredicatePushdownEnabled(
            boolean enablePredicatePushdown)
    {
        this.enablePredicatePushdown = enablePredicatePushdown;
        return this;
    }

    public boolean isMatchSubstringPushdown()
    {
        return matchSubstringPushdown;
    }

    @Config("match_substring_pushdown")
    public VastConfig setMatchSubstringPushdown(boolean matchSubstringPushdown)
    {
        this.matchSubstringPushdown = matchSubstringPushdown;
        return this;
    }

    public boolean isComplexPredicatePushdown()
    {
        return complexPredicatePushdown;
    }

    @Config("complex_predicate_pushdown")
    public VastConfig setComplexPredicatePushdown(
            boolean complexPredicatePushdown)
    {
        this.complexPredicatePushdown = complexPredicatePushdown;
        return this;
    }

    public boolean isExpressionProjectionPushdown()
    {
        return expressionProjectionPushdown;
    }

    @Config("expression_projection_pushdown")
    public VastConfig setExpressionProjectionPushdown(
            boolean expressionProjectionPushdown)
    {
        this.expressionProjectionPushdown = expressionProjectionPushdown;
        return this;
    }

    public boolean isEnableSortedProjections()
    {
        return enableSortedProjections;
    }

    @Config("enable_sorted_projections")
    public VastConfig setEnableSortedProjections(
            boolean enableSortedProjections)
    {
        this.enableSortedProjections = enableSortedProjections;
        return this;
    }

    public boolean isReportPartitioning()
    {
        return reportPartitioning;
    }

    @Config("report_partitioning")
    public VastConfig setReportPartitioning(boolean reportPartitioning)
    {
        this.reportPartitioning = reportPartitioning;
        return this;
    }

    public boolean getOnlyOrderedPushdown()
    {
        return onlyOrderedPushdown;
    }

    @Config("only_ordered_pushdown")
    public VastConfig setOnlyOrderedPushdown(boolean onlyOrderedPushdown)
    {
        this.onlyOrderedPushdown = onlyOrderedPushdown;
        return this;
    }

    @Min(1000)
    public int getMaxRowsPerInsert()
    {
        return maxRowCountPerInsert;
    }

    @Config("max_row_count_per_insert")
    public VastConfig setMaxRowsPerInsert(int maxRowCountPerInsert)
    {
        this.maxRowCountPerInsert = maxRowCountPerInsert;
        return this;
    }

    @Min(1000)
    public int getMaxRowsPerUpdate()
    {
        return maxRowCountPerUpdate;
    }

    @Config("max_row_count_per_update")
    public VastConfig setMaxRowsPerUpdate(int maxRowCountPerUpdate)
    {
        this.maxRowCountPerUpdate = maxRowCountPerUpdate;
        return this;
    }

    @Min(1000)
    public int getMaxRowsPerDelete()
    {
        return maxRowCountPerDelete;
    }

    @Config("max_row_count_per_delete")
    public VastConfig setMaxRowsPerDelete(int maxRowCountPerDelete)
    {
        this.maxRowCountPerDelete = maxRowCountPerDelete;
        return this;
    }

    @Min(1)
    public int getImportChunkLimit()
    {
        return importChunkLimit;
    }

    @Config("import_chunk_limit")
    public VastConfig setImportChunkLimit(int importChunkLimit)
    {
        this.importChunkLimit = importChunkLimit;
        return this;
    }

    @Min(1000)
    public long getMaxStatisticsFilesSupportedPerSession()
    {
        return maxStatisticsFilesSupportedPerSession;
    }

    @Config("max_statistics_files_supported_per_session")
    public VastConfig setMaxStatisticsFilesSupportedPerSession(
            long maxStatisticsFilesSupportedPerSession)
    {
        this.maxStatisticsFilesSupportedPerSession = maxStatisticsFilesSupportedPerSession;
        return this;
    }

    public boolean getKeepFilterAfterPushdown()
    {
        return keepFilterAfterPushdown;
    }

    @Config("keep_filter_after_pushdown")
    public VastConfig setKeepFilterAfterPushdown(
            boolean keepFilterAfterPushdown)
    {
        this.keepFilterAfterPushdown = keepFilterAfterPushdown;
        return this;
    }

    public boolean getVastTransactionKeepAliveEnabled()
    {
        return vastTransactionKeepAliveEnabled;
    }

    @Config("vast_transaction_keep_alive_enabled")
    public VastConfig setVastTransactionKeepAliveEnabled(
            boolean vastTransactionKeepAliveEnabled)
    {
        this.vastTransactionKeepAliveEnabled = vastTransactionKeepAliveEnabled;
        return this;
    }

    public int getVastTransactionKeepAliveIntervalSeconds()
    {
        return vastTransactionKeepAliveIntervalSeconds;
    }

    @Config("vast_transaction_keep_alive_interval_seconds")
    public VastConfig setVastTransactionKeepAliveIntervalSeconds(
            int vastTransactionKeepAliveIntervalSeconds)
    {
        this.vastTransactionKeepAliveIntervalSeconds = vastTransactionKeepAliveIntervalSeconds;
        return this;
    }

    public boolean getEstimateSplitsFromElysium()
    {
        return estimateSplitsFromElysium;
    }

    @Config("estimate_splits_from_elysium")
    public VastConfig setEstimateSplitsFromElysium(
            boolean estimateSplitsFromElysium)
    {
        this.estimateSplitsFromElysium = estimateSplitsFromElysium;
        return this;
    }

    public int getMinRowsForPartitionSplitEstimation()
    {
        return minRowsForPartitionSplitEstimation;
    }

    @Config("min_rows_for_partition_split_estimation")
    public VastConfig setMinRowsForPartitionSplitEstimation(
            int minRowsForPartitionSplitEstimation)
    {
        this.minRowsForPartitionSplitEstimation = minRowsForPartitionSplitEstimation;
        return this;
    }

    public boolean getEstimateSplitsFromRowIdPredicate()
    {
        return estimateSplitsFromRowIdPredicate;
    }

    @Config("estimate_splits_from_row_id_predicate")
    public VastConfig setEstimateSplitsFromRowIdPredicate(
            boolean estimateSplitsFromRowIdPredicate)
    {
        this.estimateSplitsFromRowIdPredicate = estimateSplitsFromRowIdPredicate;
        return this;
    }

    public Long getSeedForShufflingEndpoints()
    {
        return seedForShufflingEndpoints;
    }

    @Config("seed_for_shuffling_endpoints")
    public VastConfig setSeedForShufflingEndpoints(Long seed)
    {
        this.seedForShufflingEndpoints = seed;
        return this;
    }

    public boolean getUseColumnHistogram()
    {
        return this.useColumnHistogram;
    }

    @Config("use_column_histogram")
    public VastConfig setUseColumnHistogram(boolean useColumnHistogram)
    {
        this.useColumnHistogram = useColumnHistogram;
        return this;
    }

    public int getCompression()
    {
        return this.compression;
    }

    @Config("compression")
    public VastConfig setCompression(String compression)
    {
        if (compression.contains("zstd")) {
            this.compression = 1;
        }
        else {
            this.compression = 0;
        }
        return this;
    }

    public int getCompressionMinSavings()
    {
        return this.compressionMinSavings;
    }

    @Config("compression_min_savings")
    public VastConfig setCompressionMinSavings(int savings)
    {
        if (savings > 99) {
            this.compressionMinSavings = 99;
        }
        else {
            this.compressionMinSavings = Math.max(savings, 1);
        }
        return this;
    }

    public int getCompressionLevel()
    {
        return this.compressionLevel;
    }

    @Config("compression_level")
    public VastConfig setCompressionLevel(int level)
    {
        this.compressionLevel = level;
        return this;
    }

    public boolean getEnableAccessControl()
    {
        return enableAccessControl;
    }

    @Config(ENABLE_ACCESS_CONTROL)
    public VastConfig setEnableAccessControl(final boolean enableAccessControl)
    {
        this.enableAccessControl = enableAccessControl;
        return this;
    }

    public boolean getEnableRowColumnSecurity()
    {
        return enableRowColumnSecurity;
    }

    @Config(ENABLE_ROW_COLUMN_SECURITY)
    public VastConfig setEnableRowColumnSecurity(
            final boolean enableRowColumnSecurity)
    {
        this.enableRowColumnSecurity = enableRowColumnSecurity;
        return this;
    }

    public boolean getEnableEndUserImpersonation()
    {
        return enableEndUserImpersonation;
    }

    @Config(ENABLE_END_USER_IMPERSONATION)
    public VastConfig setEnableEndUserImpersonation(
            final boolean enableEndUserImpersonation)
    {
        this.enableEndUserImpersonation = enableEndUserImpersonation;
        return this;
    }

    public boolean isRowColumnSecurityEnabled()
    {
        if (enableRowColumnSecurity && !enableAccessControl) {
            LOG.warn(
                    "Skipping row-column-security because vast-security-control is disabled");
        }
        return enableAccessControl && enableRowColumnSecurity;
    }

    public boolean isEndUserImpersonationEnabled()
    {
        if (enableEndUserImpersonation && !enableAccessControl) {
            LOG.warn(
                    "Skipping end-user-impersonation because vast-security-control is disabled");
        }
        return enableAccessControl && enableEndUserImpersonation;
    }

    public boolean getPartitionedInsert()
    {
        return this.partitionedInsert;
    }

    @Config("partitioned_insert")
    public VastConfig setPartitionedInsert(boolean enabled)
    {
        this.partitionedInsert = enabled;
        return this;
    }

    public int getMaxInsertBuckets()
    {
        return this.maxInsertBuckets;
    }

    @Config("max_insert_buckets")
    public VastConfig setMaxInsertBuckets(int maxInsertBuckets)
    {
        this.maxInsertBuckets = maxInsertBuckets;
        return this;
    }

    public long getInsertPartitionSize()
    {
        return this.insertPartitionSize;
    }

    @Config("insert_partition_size")
    public VastConfig setInsertPartitionSize(long insertPartitionSize)
    {
        this.insertPartitionSize = insertPartitionSize;
        return this;
    }

    public boolean getInsertExactPartitioning()
    {
        return this.insertExactPartitioning;
    }

    @Config("insert_exact_partitioning")
    public VastConfig setInsertExactPartitioning(
            boolean insertExactPartitioning)
    {
        this.insertExactPartitioning = insertExactPartitioning;
        return this;
    }

    public boolean isEnableZeroRowsOptimization()
    {
        return enableZeroRowsOptimization;
    }

    @Config("enable_zero_rows_optimization")
    public VastConfig setEnableZeroRowsOptimization(
            boolean enableZeroRowsOptimization)
    {
        this.enableZeroRowsOptimization = enableZeroRowsOptimization;
        return this;
    }

    public boolean isEnablePrefillOptimization()
    {
        return enablePrefillOptimization;
    }

    @Config("enable_prefill_optimization")
    public VastConfig setEnablePrefillOptimization(
            boolean enablePrefillOptimization)
    {
        this.enablePrefillOptimization = enablePrefillOptimization;
        return this;
    }

    public boolean isEnableServerStatsCollection()
    {
        return enableServerStatsCollection;
    }

    @Config("enable_server_stats_collection")
    public VastConfig setEnableServerStatsCollection(
            boolean enableServerStatsCollection)
    {
        this.enableServerStatsCollection = enableServerStatsCollection;
        return this;
    }

    public int getShapingLoggerThreshold()
    {
        return shapingLoggerThreshold;
    }

    @Config("shaping_logger_threshold")
    public VastConfig setShapingLoggerThreshold(int shapingLoggerThreshold)
    {
        this.shapingLoggerThreshold = shapingLoggerThreshold;
        return this;
    }

    public Duration getShapingLoggerDuration()
    {
        return shapingLoggerDuration;
    }

    @Config("shaping_logger_duration")
    public VastConfig setShapingLoggerDuration(io.airlift.units.Duration value)
    {
        this.shapingLoggerDuration = Duration.ofMillis(value.toMillis());
        return this;
    }

    public int getShapingLoggerNumberOfSamples()
    {
        return shapingLoggerNumberOfSamples;
    }

    @Config("shaping_logger_number_of_samples")
    public VastConfig setShapingLoggerNumberOfSamples(
            int shapingLoggerNumberOfSamples)
    {
        this.shapingLoggerNumberOfSamples = shapingLoggerNumberOfSamples;
        return this;
    }

    public int getNodeIoExecutorNumThreads()
    {
        return nodeIoExecutorNumThreads;
    }

    @Config("node.io-executor.num-threads")
    @ConfigDescription("Number of threads in the IO executor pool")
    public VastConfig setNodeIoExecutorNumThreads(int nThreads)
    {
        this.nodeIoExecutorNumThreads = nThreads;
        return this;
    }

    public int getBufferedInserterMaxWritePermits()
    {
        return bufferedInserterMaxWritePermits;
    }

    @Config("write-buffering.max-write-permits")
    @ConfigDescription("Max in-flight writes per buffered inserter")
    public VastConfig setBufferedInserterMaxWritePermits(int nPermits)
    {
        this.bufferedInserterMaxWritePermits = nPermits;
        return this;
    }

    public int getBufferedInserterMaxJobPermits()
    {
        return bufferedInserterMaxJobPermits;
    }

    @Config("write-buffering.max-job-permits")
    @ConfigDescription("Max in-flight jobs per buffered inserter")
    public VastConfig setBufferedInserterMaxJobPermits(int nPermits)
    {
        this.bufferedInserterMaxJobPermits = nPermits;
        return this;
    }

    public DataSize getMemoryLimiterMaxAllowed()
    {
        return DataSize.succinctBytes(memoryLimiterMaxAllowed);
    }

    @Config("memory_limiter_max_allowed")
    public VastConfig setMemoryLimiterMaxAllowed(DataSize memoryLimiterMaxAllowed)
    {
        this.memoryLimiterMaxAllowed = memoryLimiterMaxAllowed.toBytes();
        return this;
    }

    public Duration getMemoryLimiterHangingValidationInterval()
    {
        return memoryLimiterHangingValidationInterval;
    }

    @Config("memory_limiter_hanging_validation_interval")
    public VastConfig setMemoryLimiterHangingValidationInterval(io.airlift.units.Duration memoryLimiterHangingValidationInterval)
    {
        this.memoryLimiterHangingValidationInterval = Duration.ofMillis(memoryLimiterHangingValidationInterval.toMillis());
        return this;
    }

    public Duration getMemoryLimiterHangingReleasePeriod()
    {
        return memoryLimiterHangingReleasePeriod;
    }

    @Config("memory_limiter_hanging_release_period")
    public VastConfig setMemoryLimiterHangingReleasePeriod(io.airlift.units.Duration memoryLimiterHangingReleasePeriod)
    {
        this.memoryLimiterHangingReleasePeriod = Duration.ofMillis(memoryLimiterHangingReleasePeriod.toMillis());
        return this;
    }

    public boolean getEnableMemoryLimit()
    {
        return enableMemoryLimit;
    }

    @Config("memory_limit_enabled")
    public VastConfig setEnableMemoryLimit(boolean enableMemoryLimit)
    {
        this.enableMemoryLimit = enableMemoryLimit;
        return this;
    }

    public int getMemoryLimitMaxNumRunnerFactor()
    {
        return memoryLimitMaxNumRunnerFactor;
    }

    @Config("memory_limit_max_num_runner_factor")
    public VastConfig setMemoryLimitMaxNumRunnerFactor(int memoryLimitMaxNumRunnerFactor)
    {
        this.memoryLimitMaxNumRunnerFactor = memoryLimitMaxNumRunnerFactor;
        return this;
    }

    public Duration getMetricDumperInterval()
    {
        return metricDumperInterval;
    }

    @Config("metric_dumper_interval")
    public VastConfig setMetricDumperInterval(io.airlift.units.Duration metricDumperInterval)
    {
        this.metricDumperInterval = Duration.ofMillis(metricDumperInterval.toMillis());
        return this;
    }

    @Override
    public String toString()
    {
        return ReflectionToStringBuilder.toString(this);
    }
}
