/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.spark.sql.connector.read.InputPartition;

import static java.lang.String.format;

public class VastInputPartition
        implements InputPartition
{
    private final String tablePath;
    private final int splitId;
    private final int batchId;
    private final int numOfSplits;

    public VastInputPartition(String tablePath, int splitId, int batchId, int numOfSplits)
    {
        this.tablePath = tablePath;
        this.splitId = splitId;
        this.batchId = batchId;
        this.numOfSplits = numOfSplits;
    }

    public int getSplitId()
    {
        return splitId;
    }

    @Override
    public String[] preferredLocations() {
        return new String[] { tablePath };
    }

    @Override
    public String toString()
    {
        return format("VastInputPartition[path=%s, batchId=%s, splitId=%s/%s]", tablePath, batchId, splitId, numOfSplits);
    }

    public int getNumOfSplits()
    {
        return this.numOfSplits;
    }
}
