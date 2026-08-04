/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino;

import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
import io.airlift.log.Logger;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static com.vastdata.client.util.NumOfSplitsEstimator.SPLITS_IDX;
import static com.vastdata.client.util.NumOfSplitsEstimator.SUBSPLITS_IDX;
import static com.vastdata.trino.GetTableSizeHelper.getTableSizeEstimate;
import static com.vastdata.trino.VastEstimatingRowsSplitSource.estimateNumOfSplits;
import static com.vastdata.trino.VastSessionProperties.getNumOfSplits;
import static com.vastdata.trino.VastSessionProperties.getNumOfSubSplits;
import static com.vastdata.trino.VastSessionProperties.getRowGroupsPerSubSplit;

public class PartitionSplitsLoader
        implements Runnable
{
    private static final Logger LOG = Logger.get(PartitionSplitsLoader.class);

    private final VastPageSourceProvider vastPageSourceProvider;
    private final VastPartitionedSplitSource partitionedSplitSource;
    private final List<HostAddress> workerAddresses;
    private final long originalEstimatedRowCount;
    private final VastTableHandle tableHandle;
    private final TableStatistics ts;
    private final TupleDomain<VastColumnHandle> predicates;
    private final VastTransaction tx;
    private final VastTraceToken token;
    private final VastSchedulingInfo schedulingInfo;
    private final ConnectorSession session;
    private final CompletableFuture<Void> completionFuture = new CompletableFuture<>();

    public PartitionSplitsLoader(VastPageSourceProvider vastPageSourceProvider,
                                 VastPartitionedSplitSource partitionedSplitSource,
                                 List<HostAddress> workerAddresses,
                                 long originalEstimatedRowCount,
                                 VastTableHandle tableHandle,
                                 TableStatistics ts,
                                 TupleDomain<VastColumnHandle> predicates,
                                 VastTransaction tx,
                                 VastTraceToken token,
                                 VastSchedulingInfo schedulingInfo,
                                 ConnectorSession session)
    {
        this.vastPageSourceProvider = vastPageSourceProvider;
        this.partitionedSplitSource = partitionedSplitSource;
        this.workerAddresses = workerAddresses;
        this.originalEstimatedRowCount = originalEstimatedRowCount;
        this.tableHandle = tableHandle;
        this.ts = ts;
        this.predicates = predicates;
        this.tx = tx;
        this.token = token;
        this.schedulingInfo = schedulingInfo;
        this.session = session;
    }

    public CompletableFuture<Void> getCompletionFuture()
    {
        return completionFuture;
    }

    @Override
    public void run()
    {
        long estimatedRowCount;
        try {
            estimatedRowCount = getTableSizeEstimate(vastPageSourceProvider,
                    tableHandle, predicates, tx, token, session);
            LOG.debug("Partition %s estimated row count: %d", predicates,
                    estimatedRowCount);
        }
        catch (Throwable t) {
            estimatedRowCount = originalEstimatedRowCount;
        }
        TableStatistics partitionTs = new TableStatistics(
                Estimate.of(estimatedRowCount), ts.getColumnStatistics());
        int numOfSubSplits = getNumOfSubSplits(session);
        if (predicates.isAll()) {
            numOfSubSplits = 1; // in full scan we don't need sub-splits
        }

        final int[] numOfSplits = estimateNumOfSplits(
                OptionalLong.of(estimatedRowCount), session, predicates, getNumOfSplits(session), numOfSubSplits,
                partitionTs, tableHandle.getLimit());

        int rowGroupsPerSubSplit = getRowGroupsPerSubSplit(session);

        LOG.debug("num of splits: %d, subSplits: %d", numOfSplits[SPLITS_IDX],
                numOfSplits[SUBSPLITS_IDX]);
        List<ConnectorSplit> splits = partitionedSplitSource.createSplits(
                numOfSplits, rowGroupsPerSubSplit, workerAddresses,
                schedulingInfo, predicates);

        partitionedSplitSource.addPartitionLoaderSplits(splits, this);
        completionFuture.complete(null);
    }

    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return this.predicates.equals(((PartitionSplitsLoader) o).predicates);
    }

    public int hashCode()
    {
        return predicates.hashCode();
    }

    @Override
    public String toString()
    {
        return "PartitionSplitsLoader{" + "tableHandle=" + tableHandle + ", predicates=" + predicates + ", token=" + token + '}';
    }
}
