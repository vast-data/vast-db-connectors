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
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastRuntimeException;
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
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.vastdata.client.util.NumOfSplitsEstimator.getNumOfSplitsEstimation;
import static com.vastdata.trino.FilterEstimator.estimateSelectivity;
import static com.vastdata.trino.FilterEstimator.splitDomains;
import static com.vastdata.trino.GetTableSizeHelper.getTableSizeEstimate;
import static com.vastdata.trino.VastSessionProperties.getAdaptivePartitioning;
import static com.vastdata.trino.VastSessionProperties.getDataEndpoints;
import static com.vastdata.trino.VastSessionProperties.getDynamicFilterCompactionThreshold;
import static com.vastdata.trino.VastSessionProperties.getDynamicFilteringWaitTimeout;
import static com.vastdata.trino.VastSessionProperties.getNumOfSplits;
import static com.vastdata.trino.VastSessionProperties.getNumOfSubSplits;
import static com.vastdata.trino.VastSessionProperties.getQueryDataRowsPerSplit;
import static com.vastdata.trino.VastSessionProperties.getRowGroupsPerSubSplit;
import static com.vastdata.trino.VastSessionProperties.getSplitSizeMultiplier;
import static com.vastdata.trino.VastSessionProperties.getEstimateSplitsFromElysium;
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
        VastTableHandle table = (VastTableHandle) connectorTableHandle;
        VastTransaction tx = (VastTransaction) transaction;
        VastTraceToken traceToken = tx.generateTraceToken(session.getTraceToken());
        VastSchedulingInfo schedulingInfo = client.getSchedulingInfo(tx, traceToken, table.getSchemaName(), table.getTableName());

        String fullTableName = format("%s/%s", table.getSchemaName(), table.getTableName());
        TableStatistics ts = statisticsManager.getTableStatistics(table).orElse(TableStatistics.empty());
        Estimate rowsEstimate = ts.getRowCount();
        TupleDomain<VastColumnHandle> tupleDomain = table.getPredicate();
        if (dynamicFilter.isComplete()) {
            TupleDomain<VastColumnHandle> dynamicPredicate = dynamicFilter
                .getCurrentPredicate()
                .transformKeys(VastColumnHandle.class::cast)
                .simplify(getDynamicFilterCompactionThreshold(session));
            tupleDomain = dynamicPredicate.intersect(tupleDomain);
        }
        if (getEstimateSplitsFromRowIdPredicate(session)) {
            rowsEstimate = vastRowsEstimator.getMinimalRowsEstimation(table.getPredicate(), rowsEstimate);
            ts = new TableStatistics(rowsEstimate, ts.getColumnStatistics());
        }
        else if (getEstimateSplitsFromElysium(session) && !tupleDomain.isAll() && !tupleDomain.isNone()) {
            Optional<List<String>> sorted = table.getSortedColumns();
            if (sorted.isPresent() && !sorted.orElseThrow().isEmpty()) {
                TupleDomain<VastColumnHandle>[] domains = splitDomains(tupleDomain, sorted.orElseThrow());
                if (!domains[0].isAll() || rowsEstimate.isUnknown()) {
                    try {
                        long estimate = getTableSizeEstimate(table, domains[0],
                                                     tx, traceToken, schedulingInfo, client, session);
                        LOG.debug("got estimate from Vast: %s (%s)", estimate, rowsEstimate.toString());
                        if (rowsEstimate.isUnknown() || rowsEstimate.getValue() > estimate) {
                            LOG.debug("updating the estimate");
                            tupleDomain = domains[1];
                            rowsEstimate = Estimate.of(estimate);
                            ts = new TableStatistics(rowsEstimate, ts.getColumnStatistics());
                        }
                    }
                    catch (VastRuntimeException re) {
                        final VastTrinoExceptionFactory vastTrinoExceptionFactory = new VastTrinoExceptionFactory();
                        throw vastTrinoExceptionFactory.fromVastRuntimeException(re);
                    }
                    catch (VastException e) {
                        final VastTrinoExceptionFactory vastTrinoExceptionFactory = new VastTrinoExceptionFactory();
                        throw vastTrinoExceptionFactory.fromVastException(e);
                    }
                }
            }
        }
        int numOfSplits = 
            estimateNumOfSplits(session, tupleDomain, ts, table.getLimit());
        LOG.debug("using %d splits for %s", numOfSplits, fullTableName);
        int numOfSubSplits = getNumOfSubSplits(session);
        int rowGroupsPerSubSplit = getRowGroupsPerSubSplit(session);

        List<VastSplit> splits = IntStream
                .range(0, numOfSplits)
                .mapToObj(currentSplit -> {
                    VastSplitContext context = new VastSplitContext(currentSplit, numOfSplits, numOfSubSplits, rowGroupsPerSubSplit);
                    return new VastSplit(getDataEndpoints(session), context, schedulingInfo);
                })
                .collect(Collectors.toList());

        return new VastSplitSource(splits, dynamicFilter, getDynamicFilteringWaitTimeout(session), fullTableName, traceToken);
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

    private static int estimateNumOfSplits(ConnectorSession session, TupleDomain<VastColumnHandle> tupleDomain,
                                           TableStatistics statistics, Optional<Long> limit)
    {
        LOG.debug("split planning parameters: %d, %b, %d, %d", getNumOfSplits(session), getAdaptivePartitioning(session),
                  getSplitSizeMultiplier(session), getQueryDataRowsPerSplit(session));
        IntSupplier maxSplitsSupplier = () -> getNumOfSplits(session);
        LongSupplier rowsPerSplitConf = () -> getQueryDataRowsPerSplit(session);
        /* The number of rows per split here should be ment from the CNode's perspective.
         * If we have a selective filter, Trino will get way less rows.
         * As a split defines both a CNode-side unit of work, and a Trino-Worker-side unit of work, here we try to
         * come up with a compromise, that is hopefully good enough for everybody.
         */
        BooleanSupplier useMultiplier = () -> !statistics.getRowCount().isUnknown() && getAdaptivePartitioning(session);
        DoubleSupplier selectivityEstimation = () -> estimateSelectivity(tupleDomain, statistics, limit);
        LongSupplier multiplierConf = () -> getSplitSizeMultiplier(session);
        Supplier<Optional<Double>> rowsEstimateSupplier =
            () -> statistics.getRowCount().isUnknown()?
            (tupleDomain.isAll() && limit.isPresent()? Optional.of(limit.orElseThrow().doubleValue()) : Optional.empty()) :
             Optional.of(statistics.getRowCount().getValue());
        LOG.debug("row estimate: %s", rowsEstimateSupplier.get());
        return getNumOfSplitsEstimation(useMultiplier, selectivityEstimation, maxSplitsSupplier,
                                        rowsPerSplitConf, multiplierConf, rowsEstimateSupplier);
    }


}
