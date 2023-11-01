/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestVastConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(VastConfig.class)
                .setEndpoint(URI.create("http://localhost:9090"))
                .setDataEndpoints("http://localhost:9090")
                .setRegion("vast")
                .setEnableCustomSchemaSeparator(false)
                .setCustomSchemaSeparator("|")
                .setAccessKeyId(null)
                .setSecretAccessKey(null)
                .setNumOfSplits(1)
                .setNumOfSubSplits(1)
                .setRowGroupsPerSubSplit(8)
                .setQueryDataRowsPerSplit(4000000)
                .setQueryDataRowsPerPage(131072)
                .setMaxRequestBodySize(5 * 1024 * 1024)
                .setRetryMaxCount(600)
                .setRetrySleepDuration(1000)
                .setParallelImport(true)
                .setMinMaxCompactionMinValuesThreshold(VastConfig.MIN_MAX_COMPACTION_MIN_VALUES_DEFAULT_VALUE)
                .setDynamicFilterCompactionThreshold(VastConfig.DYNAMIC_FILTER_COMPACTION_THRESHOLD_DEFAULT_VALUE)
                .setDynamicFilterMaxValuesThreshold(1000)
                .setDynamicFilteringWaitTimeout(2 * 1000)
                .setMatchSubstringPushdown(true)
                .setComplexPredicatePushdown(false)
                .setExpressionProjectionPushdown(false)
                .setEnableSortedProjections(true)
                .setMaxRowsPerInsert(Integer.MAX_VALUE)
                .setMaxRowsPerUpdate(2048)
                .setMaxRowsPerDelete(2048)
                .setImportChunkLimit(10)
                .setMaxStatisticsFilesSupportedPerSession(10000)
                .setKeepFilterAfterPushdown(true)
                .setVastTransactionKeepAliveIntervalSeconds(60)
                .setVastTransactionKeepAliveEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("endpoint", "file://test.json")
                .put("data_endpoints", "file://test1.json,file://test2.json")
                .put("region", "rrr")
                .put("enable_custom_schema_separator", "true")
                .put("custom_schema_separator", "---")
                .put("access_key_id", "aaa")
                .put("secret_access_key", "zzz")
                .put("num_of_splits", "4")
                .put("num_of_subsplits", "64")
                .put("rowgroups_per_subsplit", "16")
                .put("query_data_rows_per_split", "1024")
                .put("query_data_rows_per_page", "1024")
                .put("max_request_body_size", "10485760")
                .put("retry_max_count", "3")
                .put("retry_sleep_duration", "30000")
                .put("parallel_import", "false")
                .put("min_max_compaction_min_values_threshold", "300")
                .put("dynamic_filter_compaction_threshold", "1000")
                .put("dynamic_filter_max_values_threshold", "2000")
                .put("dynamic_filtering_wait_timeout", "10000")
                .put("match_substring_pushdown", "false")
                .put("complex_predicate_pushdown", "true")
                .put("expression_projection_pushdown", "true")
                .put("enable_sorted_projections", "false")
                .put("max_row_count_per_insert", "1000")
                .put("max_row_count_per_update", "2000")
                .put("max_row_count_per_delete", "3000")
                .put("import_chunk_limit", "3")
                .put("max_statistics_files_supported_per_session", "5000")
                .put("keep_filter_after_pushdown", "false")
                .put("vast_transaction_keep_alive_enabled", "false")
                .put("vast_transaction_keep_alive_interval_seconds", "150")
                .buildOrThrow();

        VastConfig expected = new VastConfig()
                .setEndpoint(URI.create("file://test.json"))
                .setDataEndpoints("file://test1.json,file://test2.json")
                .setRegion("rrr")
                .setEnableCustomSchemaSeparator(true)
                .setCustomSchemaSeparator("---")
                .setAccessKeyId("aaa")
                .setSecretAccessKey("zzz")
                .setNumOfSplits(4)
                .setNumOfSubSplits(64)
                .setRowGroupsPerSubSplit(16)
                .setQueryDataRowsPerSplit(1024)
                .setQueryDataRowsPerPage(1024)
                .setMaxRequestBodySize(10485760)
                .setRetryMaxCount(3)
                .setRetrySleepDuration(30000)
                .setParallelImport(false)
                .setMinMaxCompactionMinValuesThreshold(300)
                .setDynamicFilterCompactionThreshold(1000)
                .setDynamicFilterMaxValuesThreshold(2000)
                .setDynamicFilteringWaitTimeout(10000)
                .setMatchSubstringPushdown(false)
                .setComplexPredicatePushdown(true)
                .setExpressionProjectionPushdown(true)
                .setEnableSortedProjections(false)
                .setMaxRowsPerInsert(1000)
                .setMaxRowsPerUpdate(2000)
                .setMaxRowsPerDelete(3000)
                .setImportChunkLimit(3)
                .setMaxStatisticsFilesSupportedPerSession(5000)
                .setKeepFilterAfterPushdown(false)
                .setVastTransactionKeepAliveEnabled(false)
                .setVastTransactionKeepAliveIntervalSeconds(150);

        assertFullMapping(properties, expected);
    }
}
