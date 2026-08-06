/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.partition;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class PartitionConstants
{
    public static final String PIT_NAME_SUFFIX = "___VAST_PARTITIONS";
    public static final String PARTITION_SPEC = "VAST:partition_spec";
    public static final String TRANSFORM_KEY = "VAST:partition:transform";
    public static final String TRANSFORM_ARG = "transform-arg";
    public static final String TABULAR_PARTITION_KEY_TEMPLATE = "VAST:table:partition-key-%d";
    public static final String TABULAR_HYDRA_METADATA_KEY_FUNC = "transform";
    public static final String TABULAR_HYDRA_METADATA_KEY_COLUMN_INDEX = "column-index";

    public static final String IDENTITY_TRANSFORM = "identity";

    public static final Set<String> PIT_COLUMN_NAMES_TO_HIDE = Set.of(
            "sorting_score");
    public static final Set<String> PIT_METADATA_COLUMN_NAMES = Set.of(
            "est_row_count", "est_byte_size", "sorting_score");

    public static final Set<String> ALLOWED_TRANSFORMS = ImmutableSet.of(
            "identity", "year", "month", "day", "hour", "bucket", "truncate");
}
