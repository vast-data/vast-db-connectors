/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.util;

import io.airlift.log.Logger;

import java.util.Optional;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public final class NumOfSplitsEstimator
{
    private static final Logger LOG = Logger.get(NumOfSplitsEstimator.class);

    private NumOfSplitsEstimator() {}

    public static int estimateNumberOfSplits(IntSupplier maxSplitsSupplier, LongSupplier rowPerSplitSupplier, Supplier<Optional<Double>> rowsEstimateSupplier)
    {
        int maxNumOfSplits = maxSplitsSupplier.getAsInt();
        long rowsPerSplit = rowPerSplitSupplier.getAsLong();
        Optional<Double> rowsEstimateOpt = rowsEstimateSupplier.get();
        int estimate = getEstimate(maxNumOfSplits, rowsPerSplit, rowsEstimateOpt);
        LOG.info("Estimating num of splits: maxNumOfSplits=%d, rowsPerSplit=%d, rowsEstimateOpt=%s. Returning %d", maxNumOfSplits, rowsPerSplit, rowsEstimateOpt, estimate);
        return estimate;
    }

    private static int getEstimate(int maxNumOfSplits, long rowsPerSplit, Optional<Double> rowsEstimateOpt)
    {
        if (rowsPerSplit < 1) {
            return maxNumOfSplits;
        }
        else if (!rowsEstimateOpt.isPresent()) {
            return maxNumOfSplits;
        }
        else {
            double rows = rowsEstimateOpt.get();
            double numOfSplits = Math.max(1, Math.ceil(rows / rowsPerSplit));
            return (numOfSplits < maxNumOfSplits) ? (int) numOfSplits : maxNumOfSplits;
        }
    }

    public static Double longToDouble(long l)
    {
        return Long.valueOf(l).doubleValue();
    }
}
