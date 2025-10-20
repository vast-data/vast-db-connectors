package com.vastdata.spark;

public class VastCommonInputPartition
{
    private final String tablePath;
    private final int splitId;
    private final int batchId;
    private final int numOfSplits;

    public VastCommonInputPartition(String tablePath, int splitId, int batchId, int numOfSplits) {
        this.tablePath = tablePath;
        this.splitId = splitId;
        this.batchId = batchId;
        this.numOfSplits = numOfSplits;
    }
}
