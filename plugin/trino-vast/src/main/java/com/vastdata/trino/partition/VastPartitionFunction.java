/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.partition;

import com.vastdata.client.partition.PartitionColumnMetadata;
import io.trino.spi.type.Type;

import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// NOTE: the partitioning function must contain the data path for nested fields because the partitioning columns
// reference the top level column, not the nested column. This means that even thought the partitioning functions
// with a different path should be compatible, the system does not consider them compatible. Fortunately, partitioning
// on nested columns is not common.
public record VastPartitionFunction(TransformType transform,
        Type type,
        OptionalInt size,
        String columnName,
        Integer partitionIdx,
        Integer columnIdx)
{
    public VastPartitionFunction(TransformType transform,
                                 Type type,
                                 String columnName,
                                 Integer partitionIdx,
                                 Integer columnIdx)
    {
        this(transform, type, OptionalInt.empty(), columnName, partitionIdx,
                columnIdx);
    }

    public VastPartitionFunction
    {
        requireNonNull(transform, "transform is null");
        requireNonNull(partitionIdx, "partitionIdx is null");
        requireNonNull(columnIdx, "columnIdx is null");
        requireNonNull(type, "type is null");
        requireNonNull(size, "size is null");
        checkArgument(size.orElse(0) >= 0,
                "size must be greater than or equal to zero");
        checkArgument(
                size.isEmpty() || transform == TransformType.BUCKET || transform == TransformType.TRUNCATE,
                "size is only valid for BUCKET and TRUNCATE transforms");
    }

    public static VastPartitionFunction create(String transform,
                                               OptionalInt transformArg,
                                               Type type,
                                               String columnName,
                                               Integer partitionIdx,
                                               Integer columnIdx)
    {
        return switch (transform.toLowerCase(java.util.Locale.ENGLISH)) {
            case "identity" ->
                    new VastPartitionFunction(TransformType.IDENTITY, type,
                            columnName, partitionIdx, columnIdx);
            case "year" -> new VastPartitionFunction(TransformType.YEAR, type,
                    columnName, partitionIdx, columnIdx);
            case "month" -> new VastPartitionFunction(TransformType.MONTH, type,
                    columnName, partitionIdx, columnIdx);
            case "day" -> new VastPartitionFunction(TransformType.DAY, type,
                    columnName, partitionIdx, columnIdx);
            case "hour" -> new VastPartitionFunction(TransformType.HOUR, type,
                    columnName, partitionIdx, columnIdx);
            case "bucket" ->
                    new VastPartitionFunction(TransformType.BUCKET, type,
                            transformArg, columnName, partitionIdx, columnIdx);
            case "truncate" ->
                    new VastPartitionFunction(TransformType.TRUNCATE, type,
                            transformArg, columnName, partitionIdx, columnIdx);
            default -> {
                throw new UnsupportedOperationException(
                        "Unsupported partition transform: " + transform);
            }
        };
    }

    public static VastPartitionFunction create(PartitionColumnMetadata pcm,
                                               Type type,
                                               Integer partitionIdx,
                                               Integer columnIdx)
    {
        OptionalInt transformArg = pcm.arg == null ?
                OptionalInt.empty() :
                OptionalInt.of(pcm.arg);
        return create(pcm.transform, transformArg, type, pcm.sourceColumnName,
                partitionIdx, columnIdx);
    }
}
