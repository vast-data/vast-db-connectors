/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;

import java.util.List;

public class VastNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private static final Logger LOG = Logger.get(
            VastNodePartitioningProvider.class);

    private final TypeOperators typeOperators;

    @Inject
    public VastNodePartitioningProvider(TypeManager typeManager)
    {
        this.typeOperators = typeManager.getTypeOperators();
    }

    public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle,
                                            ConnectorSession session,
                                            ConnectorPartitioningHandle partitioningHandle,
                                            List<Type> partitionChannelTypes,
                                            int bucketCount)
    {
        return new VastBucketFunction(
                (VastPartitioningHandle) partitioningHandle, typeOperators,
                bucketCount);
    }
}
