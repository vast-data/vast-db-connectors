/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.spark.sql.connector.read.InputPartition;

import static java.lang.String.format;

public class VastInputPartition
        implements InputPartition
{
    private final int splitId;
    private final int batchId;
    private final int numOfSplits;

    public VastInputPartition(int splitId, int batchId, int numOfSplits)
    {
        this.splitId = splitId;
        this.batchId = batchId;
        this.numOfSplits = numOfSplits;
    }

    public int getSplitId()
    {
        return splitId;
    }

    @Override
    public String toString()
    {
        return format("VastInputPartition[batchId=%s, splitId=%s/%s]", batchId, splitId, numOfSplits);
    }

    public int getNumOfSplits()
    {
        return this.numOfSplits;
    }
}
