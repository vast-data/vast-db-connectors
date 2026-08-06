/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.trino.partition.PartitionKeyHashFunction;
import com.vastdata.trino.partition.PartitionKeyHashFunction.IndexBase;
import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.TypeOperators;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class VastBucketFunction
        implements BucketFunction
{
    private static final Logger LOG = Logger.get(VastBucketFunction.class);

    private final int bucketCount;
    private final PartitionKeyHashFunction hashFunction;

    public VastBucketFunction(VastPartitioningHandle partitioningHandle,
                              TypeOperators typeOperators,
                              int bucketCount)
    {
        requireNonNull(partitioningHandle, "partitioningHandle is null");
        requireNonNull(typeOperators, "typeOperators is null");
        checkArgument(bucketCount > 0, "Invalid bucketCount: %s", bucketCount);

        this.bucketCount = bucketCount;

        hashFunction = PartitionKeyHashFunction.create(
                partitioningHandle.partitionFunctions(), typeOperators,
                IndexBase.BY_PARTITION_INDEX);
    }

    public int getBucket(Page page, int position)
    {
        // TODO in iceberg there is a single bucket optimization right here

        long nonNegativeHash = hashFunction.apply(page,
                position) & Long.MAX_VALUE;
        return (int) (nonNegativeHash % bucketCount);
    }
}
