/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.spark.predicate.VastPredicate;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.HasPartitionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class VastKeyGroupedInputPartition
        extends VastInputPartition
        implements HasPartitionKey
{
    private static final Logger LOG = LoggerFactory.getLogger(
            VastKeyGroupedInputPartition.class);
    private final InternalRow partitionKey;
    private final List<List<VastPredicate>> predicate;

    public VastKeyGroupedInputPartition(
            List<List<VastPredicate>> partitionPredicates,
            int splitId,
            int batchId,
            int numOfSplits,
            int numOfSubSplits,
            InternalRow key)
    {
        super(splitId, batchId, numOfSplits, numOfSubSplits);
        partitionKey = key.copy();
        predicate = partitionPredicates;
    }

    @Override
    public InternalRow partitionKey()
    {
        return this.partitionKey;
    }

    @Override
    public List<List<VastPredicate>> partitionPredicate()
    {
        LOG.debug("partitionPredicate: {}", predicate);
        return predicate;
    }
}
