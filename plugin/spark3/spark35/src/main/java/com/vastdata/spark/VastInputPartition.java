/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.spark.predicate.VastPredicate;

import org.apache.spark.sql.connector.read.InputPartition;

import java.util.List;

import static java.lang.String.format;

public class VastInputPartition
        implements InputPartition
{
    private final int splitId;
    private final int batchId;
    private final int numOfSplits;
    private final int numOfSubSplits;

    public VastInputPartition(int splitId, int batchId, int numOfSplits,
            int numOfSubSplits)
    {
        this.splitId = splitId;
        this.batchId = batchId;
        this.numOfSplits = numOfSplits;
        this.numOfSubSplits = numOfSubSplits;
    }

    public int getSplitId()
    {
        return splitId;
    }

    @Override
    public String toString()
    {
        return format("VastInputPartition[batchId=%s, splitId=%s/%s]", batchId,
                splitId, numOfSplits);
    }

    public int getNumOfSplits()
    {
        return this.numOfSplits;
    }

    public int getNumOfSubSplits()
    {
        return this.numOfSubSplits;
    }

    public List<List<VastPredicate>> partitionPredicate()
    {
        return null;
    }
}
