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

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class VastConfig
        implements Serializable
{
    public static final int MIN_SUB_SPLITS = 1;
    public static final int MAX_SUB_SPLITS = 64;

    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private URI endpoint = URI.create("http://localhost:9090");
    private List<URI> dataEndpoints; // optional endpoints for data-related queries
    private String accessKeyId;
    private String secretAccessKey;
    private String region = "vast";

    private int numOfSplits = 1;
    private int numOfSubSplits = 1;
    private int rowGroupsPerSubSplit = 1;
    private long queryDataRowsPerSplit = 4 * 1000 * 1000; // allow dynamic splits (256 splits per 1B rows)
    private int queryDataRowsPerPage = 128 * 1024; // should be a multiple of a row group size (2 ** 16 rows)
    private long maxRequestBodySize = 5 * 1024 * 1024; // must be <= Mooktze largest buffer size (see `src/plasma/execution/silo.cpp`)

    private int retryMaxCount = 600; // 10 minutes of retries in case we can't connect to VAST
    private int retrySleepDuration = 1000;
    private boolean parallelImport = true;
    private int dynamicFilteringWaitTimeout = 2 * 1000;

    private int dynamicFilterCompactionThreshold = 100;

    private String engineVersion = "NA";

    private boolean complexPredicatePushdown = false;
    private boolean expressionProjectionPushdown = false;

    private int maxRowCountPerInsert = Integer.MAX_VALUE;
    private int maxRowCountPerUpdate = 2048;
    private int maxRowCountPerDelete = 2048;

    private int importChunkLimit = EvenSizeWithLimitChunkifier.CHUNK_SIZE_LIMIT;

    private long maxStatisticsFilesSupportedPerSession = 10000;
    private boolean keepFilterAfterPushdown = true;

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
        return Optional.ofNullable(dataEndpoints).orElse(ImmutableList.of(endpoint));
    }

    @Config("data_endpoints")
    public VastConfig setDataEndpoints(String dataEndpoints)
    {
        this.dataEndpoints = SPLITTER.splitToStream(dataEndpoints).map(URI::create).collect(Collectors.toList());
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


    @Config(("max_request_body_size"))
    public VastConfig setMaxRequestBodySize(long maxRequestBodySize)
    {
        this.maxRequestBodySize = maxRequestBodySize;
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

    @NotNull
    public int getDynamicFilteringWaitTimeout()
    {
        return dynamicFilteringWaitTimeout;
    }

    @Config("dynamic_filtering_wait_timeout")
    @ConfigDescription("Duration to wait for completion of dynamic filters during split generation")
    public VastConfig setDynamicFilteringWaitTimeout(int dynamicFilteringWaitTimeout)
    {
        this.dynamicFilteringWaitTimeout = dynamicFilteringWaitTimeout;
        return this;
    }

    @Config("parallel_import")
    public VastConfig setParallelImport(boolean parallelImport)
    {
        this.parallelImport = parallelImport;
        return this;
    }

    public int getDynamicFilterCompactionThreshold()
    {
        return dynamicFilterCompactionThreshold;
    }

    @Config("dynamic_filter_compaction_threshold")
    public VastConfig setDynamicFilterCompactionThreshold(int dynamicFilterCompactionThreshold)
    {
        this.dynamicFilterCompactionThreshold = dynamicFilterCompactionThreshold;
        return this;
    }

    public String getEngineVersion()
    {
        return engineVersion;
    }

    public VastConfig setEngineVersion(String engineVersion)
    {
        this.engineVersion = engineVersion;
        return this;
    }

    public boolean isComplexPredicatePushdown()
    {
        return complexPredicatePushdown;
    }

    @Config("complex_predicate_pushdown")
    public VastConfig setComplexPredicatePushdown(boolean complexPredicatePushdown)
    {
        this.complexPredicatePushdown = complexPredicatePushdown;
        return this;
    }

    public boolean isExpressionProjectionPushdown()
    {
        return expressionProjectionPushdown;
    }

    @Config("expression_projection_pushdown")
    public VastConfig setExpressionProjectionPushdown(boolean expressionProjectionPushdown)
    {
        this.expressionProjectionPushdown = expressionProjectionPushdown;
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
    public long getMaxStatisticsFilesSupportedPerSession() {return maxStatisticsFilesSupportedPerSession;}

    @Config("max_statistics_files_supported_per_session")
    public VastConfig setMaxStatisticsFilesSupportedPerSession(long maxStatisticsFilesSupportedPerSession)
    {
        this.maxStatisticsFilesSupportedPerSession = maxStatisticsFilesSupportedPerSession;
        return this;
    }

    public boolean getKeepFilterAfterPushdown()
    {
        return keepFilterAfterPushdown;
    }

    @Config("keep_filter_after_pushdown")
    public VastConfig setKeepFilterAfterPushdown(boolean keepFilterAfterPushdown)
    {
        this.keepFilterAfterPushdown = keepFilterAfterPushdown;
        return this;
    }
}
