/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.util;

import io.airlift.log.Logger;

import java.util.OptionalLong;

import static java.lang.Math.max;
import static java.lang.Math.min;

public final class NumOfSplitsEstimator
{
    public static final int SPLITS_IDX = 0;
    public static final int SUBSPLITS_IDX = 1;
    private static final Logger LOG = Logger.get(NumOfSplitsEstimator.class);

    private NumOfSplitsEstimator()
    {
    }

    public static int[] getNumOfSplitsEstimation(OptionalLong rowEstimate,
            int maxNumOfSplits, int maxNumOfSubSplits, int rowGroupsPerSubSplit,
            int rowsPerPage, double estimatedSelectivity, long rowsPerSplitConf,
            boolean adaptivePartitioning)
    {
        LOG.debug(
                "getNumOfSplitsEstimation: split planning parameters: %s, %s, %s, %s",
                maxNumOfSplits, rowsPerSplitConf, rowEstimate,
                estimatedSelectivity);
        if (!rowEstimate.isPresent() || !adaptivePartitioning) {
            return new int[] {maxNumOfSplits, maxNumOfSubSplits};
        }
        rowGroupsPerSubSplit = (rowsPerPage >> 16) > rowGroupsPerSubSplit ?
                (rowsPerPage >> 16) :
                rowGroupsPerSubSplit;
        final long minRowsPerSplit = (long) rowGroupsPerSubSplit << 16;
        final int maxVastSplits = (int) (rowEstimate.getAsLong() / minRowsPerSplit) + (
                rowEstimate.getAsLong() % minRowsPerSplit > 0 ?
                        1 :
                        0);
        LOG.info("getNumOfSplitsEstimation: maxVastSplits: %s", maxVastSplits);
        if (maxVastSplits <= 1) {                            // small dim table
            return new int[] {1, 1};
        }
        final int minVastSplits = maxVastSplits / maxNumOfSubSplits + (
                maxVastSplits % maxNumOfSubSplits == 0 ?
                        0 :
                        1);
        final long estimatedRows = (long) (rowEstimate.getAsLong() * estimatedSelectivity);
        int estimatedSplits = (int) (estimatedRows / rowsPerSplitConf) + (
                estimatedRows % rowsPerSplitConf > 0 ?
                        1 :
                        0);
        estimatedSplits = min(estimatedSplits, maxVastSplits);
        estimatedSplits = max(estimatedSplits, minVastSplits);
        estimatedSplits = min(estimatedSplits, maxNumOfSplits);

        int subsplits = maxVastSplits / estimatedSplits + (maxVastSplits % estimatedSplits > 0 ?
                1 :
                0);
        subsplits = min(subsplits, maxNumOfSubSplits);

        LOG.info("getNumOfSplitsEstimation: split planning results: %s, %s",
                estimatedSplits, subsplits);

        return new int[] {estimatedSplits, subsplits};
    }
}
