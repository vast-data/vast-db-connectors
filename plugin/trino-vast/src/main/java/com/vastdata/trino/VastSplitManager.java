/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.VastSplitContext;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.trino.statistics.VastStatisticsManager;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.vastdata.client.util.NumOfSplitsEstimator.estimateNumberOfSplits;
import static com.vastdata.trino.VastSessionProperties.getDataEndpoints;
import static com.vastdata.trino.VastSessionProperties.getDynamicFilteringWaitTimeout;
import static com.vastdata.trino.VastSessionProperties.getNumOfSplits;
import static com.vastdata.trino.VastSessionProperties.getNumOfSubSplits;
import static com.vastdata.trino.VastSessionProperties.getQueryDataRowsPerSplit;
import static com.vastdata.trino.VastSessionProperties.getRowGroupsPerSubSplit;
import static com.vastdata.trino.VastSessionProperties.getEstimateSplitsFromRowIdPredicate;
import static java.lang.String.format;

public class VastSplitManager
        implements ConnectorSplitManager
{
    private static final Logger LOG = Logger.get(VastSplitManager.class);
    private static final ConnectorSplitSource.ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitSource.ConnectorSplitBatch(ImmutableList.of(), false);

    private final VastClient client;
    private final VastStatisticsManager statisticsManager;
    private final VastRowsEstimator vastRowsEstimator = new VastRowsEstimator();

    @Inject
    public VastSplitManager(VastClient client, VastStatisticsManager statisticsManager)
    {
        this.client = client;
        this.statisticsManager = statisticsManager;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        VastTransaction vastTransaction = (VastTransaction) transaction;
        VastTableHandle table = (VastTableHandle) connectorTableHandle;

        String fullTableName = format("%s/%s", table.getSchemaName(), table.getTableName());
        Estimate rowsEstimate = statisticsManager.getTableStatistics(table).orElse(TableStatistics.empty()).getRowCount();
        if (getEstimateSplitsFromRowIdPredicate(session)) {
            rowsEstimate = vastRowsEstimator.getMinimalRowsEstimation(table.getPredicate(), rowsEstimate);
        }
        int numOfSplits = estimateNumOfSplits(session, rowsEstimate);
        LOG.debug("using %d splits for %s, estimated to have %s rows", numOfSplits, fullTableName, rowsEstimate);
        int numOfSubSplits = getNumOfSubSplits(session);
        int rowGroupsPerSubSplit = getRowGroupsPerSubSplit(session);
        VastTransaction tx = (VastTransaction) transaction;
        VastTraceToken traceToken = tx.generateTraceToken(session.getTraceToken());
        VastSchedulingInfo schedulingInfo = client.getSchedulingInfo(tx, traceToken, table.getSchemaName(), table.getTableName());

        List<VastSplit> splits = IntStream
                .range(0, numOfSplits)
                .mapToObj(currentSplit -> {
                    VastSplitContext context = new VastSplitContext(currentSplit, numOfSplits, numOfSubSplits, rowGroupsPerSubSplit);
                    return new VastSplit(getDataEndpoints(session), context, schedulingInfo);
                })
                .collect(Collectors.toList());

        return new VastSplitSource(splits, dynamicFilter, getDynamicFilteringWaitTimeout(session), fullTableName, vastTransaction.generateTraceToken(session.getTraceToken()));
    }

    private static class VastSplitSource
            extends FixedSplitSource
    {
        private final DynamicFilter dynamicFilter;
        private final Stopwatch dynamicFilterWaitStopwatch;
        private final long dynamicFilteringWaitTimeoutMillis;
        private final String fullTableName;
        private final VastTraceToken traceToken;

        public VastSplitSource(List<VastSplit> splits, DynamicFilter dynamicFilter, int dynamicFilterTimeout, String fullTableName, VastTraceToken traceToken)
        {
            super(splits);
            this.dynamicFilter = dynamicFilter;
            this.dynamicFilterWaitStopwatch = Stopwatch.createStarted();
            this.dynamicFilteringWaitTimeoutMillis = dynamicFilterTimeout;
            this.fullTableName = fullTableName;
            this.traceToken = traceToken;
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
        {
            long timeLeft = dynamicFilteringWaitTimeoutMillis - dynamicFilterWaitStopwatch.elapsed(TimeUnit.MILLISECONDS);
            if (dynamicFilter.isAwaitable() && timeLeft > 0) {
                LOG.debug("QueryData(%s) blocking %s split generation for %.3f seconds", traceToken, fullTableName, timeLeft / 1e3);
                return dynamicFilter.isBlocked()
                        .thenApply(ignored -> EMPTY_BATCH)
                        .completeOnTimeout(EMPTY_BATCH, timeLeft, TimeUnit.MILLISECONDS);
            }
            LOG.debug("QueryData(%s) getting %d splits from %s", traceToken, maxSize, fullTableName);
            return super.getNextBatch(maxSize);
        }
    }

    private static int estimateNumOfSplits(ConnectorSession session, Estimate rowsEstimate)
    {
        IntSupplier maxSplitsSupplier = () -> getNumOfSplits(session);
        LongSupplier rowPerSplitSupplier = () -> getQueryDataRowsPerSplit(session);
        Supplier<Optional<Double>> rowsEstimateSupplier = () -> rowsEstimate.isUnknown() ? Optional.empty() : Optional.of(rowsEstimate.getValue());
        return estimateNumberOfSplits(maxSplitsSupplier, rowPerSplitSupplier, () -> -1L, rowsEstimateSupplier, 0);
    }


}
