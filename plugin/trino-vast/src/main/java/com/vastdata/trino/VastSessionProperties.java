/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.vastdata.client.VastConfig;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.List;
import java.util.Optional;
import static com.vastdata.client.VastConfig.MAX_SUB_SPLITS;
import static com.vastdata.client.VastConfig.MIN_SUB_SPLITS;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class VastSessionProperties
        implements SessionPropertiesProvider
{
    private static final String NUM_OF_SPLITS = "num_of_splits";
    private static final String NUM_OF_SUBSPLITS = "num_of_subsplits";
    private static final String ROWGROUPS_PER_SUBSPLIT = "rowgroups_per_subsplit";
    private static final String QUERY_DATA_ROWS_PER_SPLIT = "query_data_rows_per_split";
    private static final String QUERY_DATA_ROWS_PER_PAGE = "query_data_rows_per_page";
    private static final String CLIENT_PAGE_SIZE = "client_page_size";
    private static final String DATA_ENDPOINTS = "data_endpoints";

    private static final String ENABLE_CUSTOM_SCHEMA_SEPARATOR = "enable_custom_schema_separator";
    private static final String CUSTOM_SCHEMA_SEPARATOR = "custom_schema_separator";

    private static final String RETRY_MAX_COUNT = "retry_max_count";
    private static final String RETRY_SLEEP_DURATION = "retry_sleep_duration";
    private static final String PARALLEL_IMPORT_DURATION = "parallel_import";
    private static final String DYNAMIC_FILTER_COMPACTION_THRESHOLD = "dynamic_filter_compaction_threshold";
    private static final String DYNAMIC_FILTERING_WAIT_TIMEOUT = "dynamic_filtering_wait_timeout";

    private static final String DEBUG_DISABLE_ARROW_PARSING = "debug_disable_arrow_parsing";
    private static final String DEBUG_DISABLE_PAGE_QUEUEING = "debug_disable_page_queueing";

    private static final String MATCH_SUBSTRING_PUSHDOWN = "match_substring_pushdown";
    private static final String COMPLEX_PREDICATE_PUSHDOWN = "complex_predicate_pushdown";
    private static final String EXPRESSION_PROJECTION_PUSHDOWN = "expression_projection_pushdown";
    private static final String ENABLE_SORTED_PROJECTIONS = "enable_sorted_projections";

    private static final String MAX_ROWS_PER_INSERT = "max_rows_per_insert";
    private static final String MAX_ROWS_PER_UPDATE = "max_rows_per_update";
    private static final String MAX_ROWS_PER_DELETE = "max_rows_per_delete";
    private static final String IMPORT_CHUNK_LIMIT = "import_chunk_limit";

    private static final String ESTIMATE_SPLITS_FROM_ROW_ID_PREDICATE = "estimate_splits_from_row_id_predicate";
    private static final String ESTIMATE_SPLITS_FROM_ELYSIUM = "estimate_splits_from_elysium";

    private static final String ADAPTIVE_PARTITIONING_PREDICATE = "adaptive_partitioning";
    private static final String SPLIT_SIZE_MULTIPLIER = "split_size_multiplier";

    private static final String SEED_FOR_SHUFFLING_ENDPOINTS = "seed_for_shuffling_endpoints";

    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public VastSessionProperties(VastConfig config)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(integerProperty(
                        NUM_OF_SPLITS,
                        "Number of splits (per table)",
                        config.getNumOfSplits(),
                        false))
                .add(integerProperty(
                        NUM_OF_SUBSPLITS,
                        "Number of sub-splits (per split)",
                        config.getNumOfSubSplits(),
                        integerBetween(MIN_SUB_SPLITS, MAX_SUB_SPLITS),
                        false))
                .add(integerProperty(
                        ROWGROUPS_PER_SUBSPLIT,
                        "Number of rowgroups (per sub-split)",
                        config.getRowGroupsPerSubSplit(),
                        false))
                .add(longProperty(
                        QUERY_DATA_ROWS_PER_SPLIT,
                        "If positive, allows reducing splits' number for smaller tables (from `num_of_splits`)",
                        config.getQueryDataRowsPerSplit(),
                        false))
                .add(integerProperty(
                        QUERY_DATA_ROWS_PER_PAGE,
                        "Number of rows per QueryData page",
                        config.getQueryDataRowsPerPage(),
                        false))
                .add(integerProperty(
                        CLIENT_PAGE_SIZE,
                        "Vast client page size (for metadata requests)",
                        1000,
                        false))
                .add(new PropertyMetadata<>(
                        DATA_ENDPOINTS,
                        "Vast endpoints for data-intensive queries",
                        VARCHAR,
                        List.class,
                        config.getDataEndpoints(),
                        false,
                        value -> SPLITTER.splitToStream((String) value).map(URI::create).collect(Collectors.toList()),
                        endpoints -> endpoints
                                .stream()
                                .map(Object::toString)
                                .collect(Collectors.joining(","))))
                .add(booleanProperty(
                        ENABLE_CUSTOM_SCHEMA_SEPARATOR,
                        "Replace `/` bucket-schema separator with the specified value",
                        config.getEnableCustomSchemaSeparator(),
                        false))
                .add(stringProperty(
                        CUSTOM_SCHEMA_SEPARATOR,
                        "Custom separator between bucket and schemas' names",
                        config.getCustomSchemaSeparator(),
                        false))
                .add(integerProperty(
                        RETRY_MAX_COUNT,
                        "Maximal number of retries",
                        config.getRetryMaxCount(),
                        false))
                .add(integerProperty(
                        RETRY_SLEEP_DURATION,
                        "Sleep duration between retries",
                        config.getRetrySleepDuration(),
                        false))
                .add(booleanProperty(
                        PARALLEL_IMPORT_DURATION,
                        "Vast Import data parallel execution",
                        config.getParallelImport(),
                        false))
                .add(integerProperty(
                        DYNAMIC_FILTER_COMPACTION_THRESHOLD,
                        "Maximum ranges to allow in per column domain without compacting it",
                        config.getDynamicFilterCompactionThreshold(),
                        false))
                .add(integerProperty(
                        DYNAMIC_FILTERING_WAIT_TIMEOUT,
                        "Duration to wait for completion of dynamic filters during split generation",
                        config.getDynamicFilteringWaitTimeout(),
                        false))
                .add(booleanProperty(
                        MATCH_SUBSTRING_PUSHDOWN,
                        "Enable `match_substring` pushdown (via `col LIKE '%substring%'`)",
                        config.isMatchSubstringPushdown(),
                        false))
                .add(booleanProperty(
                        COMPLEX_PREDICATE_PUSHDOWN,
                        "Enable complex predicate pushdown (with `OR` between columns)",
                        config.isComplexPredicatePushdown(),
                        false))
                .add(booleanProperty(
                        EXPRESSION_PROJECTION_PUSHDOWN,
                        "Enable expression projection pushdown (e.g. col IS NOT NULL)",
                        config.isExpressionProjectionPushdown(),
                        false))
                .add(booleanProperty(
                        ENABLE_SORTED_PROJECTIONS,
                        "Enable sorted projections usage during QueryData",
                        config.isEnableSortedProjections(),
                        false))
                .add(booleanProperty(
                        DEBUG_DISABLE_ARROW_PARSING,
                        "Debug: disable page building in QueryData",
                        false,
                        true))
                .add(booleanProperty(
                        DEBUG_DISABLE_PAGE_QUEUEING,
                        "Debug: disable page queueing in QueryData",
                        false,
                        true))
                .add(integerProperty(
                        MAX_ROWS_PER_INSERT,
                        "Maximum number of rows in InsertRows RPC",
                        config.getMaxRowsPerInsert(),
                        false))
                .add(integerProperty(
                        MAX_ROWS_PER_UPDATE,
                        "Maximum number of rows in UpdateRows RPC",
                        config.getMaxRowsPerUpdate(),
                        false))
                .add(integerProperty(
                        MAX_ROWS_PER_DELETE,
                        "Maximum number of rows in DeleteRows RPC",
                        config.getMaxRowsPerDelete(),
                        false))
                .add(integerProperty(
                        IMPORT_CHUNK_LIMIT,
                        "Number of files' limit per ImportData RPC",
                        config.getImportChunkLimit(),
                        false))
                .add(booleanProperty(
                        ESTIMATE_SPLITS_FROM_ROW_ID_PREDICATE,
                        "Number of splits will be calculated with consideration to the user defined ROW_IDs",
                        config.getEstimateSplitsFromRowIdPredicate(),
                        false))
                .add(booleanProperty(
                        ESTIMATE_SPLITS_FROM_ELYSIUM,
                        "Number of splits will be calculated with consideration to the sorting key",
                        config.getEstimateSplitsFromElysium(),
                        false))
                .add(longProperty(
                        SEED_FOR_SHUFFLING_ENDPOINTS,
                        "Seed for shuffling the data endpoints deterministically",
                        config.getSeedForShufflingEndpoints(),
                        true))
                .add(booleanProperty(
                        ADAPTIVE_PARTITIONING_PREDICATE,
                        "Determine nuber of splits based on table statistics",
                        config.getAdaptivePartitioning(),
                        true))
                .add(integerProperty(
                        SPLIT_SIZE_MULTIPLIER,
                        "Increase the split size accordingly when running with selective filters",
                        config.getSplitSizeMultiplier(),
                        true))
                .build();
    }

    private Consumer<Integer> integerBetween(int min, int max)
    {
        return value -> {
            if (value >= min && value <= max) {
                return;
            }
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s must be between [%d, %d]", value, min, max));
        };
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static int getNumOfSplits(ConnectorSession session)
    {
        return session.getProperty(NUM_OF_SPLITS, Integer.class);
    }

    public static int getNumOfSubSplits(ConnectorSession session)
    {
        return session.getProperty(NUM_OF_SUBSPLITS, Integer.class);
    }

    public static int getRowGroupsPerSubSplit(ConnectorSession session)
    {
        return session.getProperty(ROWGROUPS_PER_SUBSPLIT, Integer.class);
    }

    public static long getQueryDataRowsPerSplit(ConnectorSession session)
    {
        return session.getProperty(QUERY_DATA_ROWS_PER_SPLIT, Long.class);
    }

    public static int getQueryDataRowsPerPage(ConnectorSession session)
    {
        return session.getProperty(QUERY_DATA_ROWS_PER_PAGE, Integer.class);
    }

    public static int getClientPageSize(ConnectorSession session)
    {
        return session.getProperty(CLIENT_PAGE_SIZE, Integer.class);
    }

    @SuppressWarnings("unchecked")
    public static List<URI> getDataEndpoints(ConnectorSession session)
    {
        return (List<URI>) session.getProperty(DATA_ENDPOINTS, List.class);
    }

    public static Optional<Long> getSeedForShufflingEndpoints(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(SEED_FOR_SHUFFLING_ENDPOINTS, Long.class));
    }

    public static boolean getEnableCustomSchemaSeparator(ConnectorSession session)
    {
        return session.getProperty(ENABLE_CUSTOM_SCHEMA_SEPARATOR, Boolean.class);
    }

    public static String getCustomSchemaSeparator(ConnectorSession session)
    {
        return session.getProperty(CUSTOM_SCHEMA_SEPARATOR, String.class);
    }

    public static int getRetryMaxCount(ConnectorSession session)
    {
        return session.getProperty(RETRY_MAX_COUNT, Integer.class);
    }

    public static int getRetrySleepDuration(ConnectorSession session)
    {
        return session.getProperty(RETRY_SLEEP_DURATION, Integer.class);
    }

    public static boolean getParallelImport(ConnectorSession session)
    {
        return session.getProperty(PARALLEL_IMPORT_DURATION, Boolean.class);
    }

    public static int getDynamicFilterCompactionThreshold(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTER_COMPACTION_THRESHOLD, Integer.class);
    }

    public static int getDynamicFilteringWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT, Integer.class);
    }

    public static boolean getDebugDisableArrowParsing(ConnectorSession session)
    {
        return session.getProperty(DEBUG_DISABLE_ARROW_PARSING, Boolean.class);
    }

    public static boolean getDebugDisablePageQueueing(ConnectorSession session)
    {
        return session.getProperty(DEBUG_DISABLE_PAGE_QUEUEING, Boolean.class);
    }

    public static boolean getMatchSubstringPushdown(ConnectorSession session)
    {
        return session.getProperty(MATCH_SUBSTRING_PUSHDOWN, Boolean.class);
    }

    public static boolean getComplexPredicatePushdown(ConnectorSession session)
    {
        return session.getProperty(COMPLEX_PREDICATE_PUSHDOWN, Boolean.class);
    }

    public static boolean getExpressionProjectionPushdown(ConnectorSession session)
    {
        return session.getProperty(EXPRESSION_PROJECTION_PUSHDOWN, Boolean.class);
    }

    public static boolean getEnableSortedProjections(ConnectorSession session)
    {
        return session.getProperty(ENABLE_SORTED_PROJECTIONS, Boolean.class);
    }

    public static int getMaxRowsPerInsert(ConnectorSession session)
    {
        return session.getProperty(MAX_ROWS_PER_INSERT, Integer.class);
    }

    public static int getMaxRowsPerUpdate(ConnectorSession session)
    {
        return session.getProperty(MAX_ROWS_PER_UPDATE, Integer.class);
    }

    public static int getMaxRowsPerDelete(ConnectorSession session)
    {
        return session.getProperty(MAX_ROWS_PER_DELETE, Integer.class);
    }

    public static int getImportChunkLimit(ConnectorSession session)
    {
        return session.getProperty(IMPORT_CHUNK_LIMIT, Integer.class);
    }

    public static boolean getEstimateSplitsFromRowIdPredicate(ConnectorSession session)
    {
        return session.getProperty(ESTIMATE_SPLITS_FROM_ROW_ID_PREDICATE, Boolean.class);
    }

    public static boolean getEstimateSplitsFromElysium(ConnectorSession session)
    {
        return session.getProperty(ESTIMATE_SPLITS_FROM_ELYSIUM, Boolean.class);
    }

    public static boolean getAdaptivePartitioning(ConnectorSession session)
    {
        return session.getProperty(ADAPTIVE_PARTITIONING_PREDICATE, Boolean.class);
    }

    public static int getSplitSizeMultiplier(ConnectorSession session)
    {
        return session.getProperty(SPLIT_SIZE_MULTIPLIER, Integer.class);
    }
}
