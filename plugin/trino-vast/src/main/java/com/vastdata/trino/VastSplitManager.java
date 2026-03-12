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
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.vastdata.client.util.NumOfSplitsEstimator.getNumOfSplitsEstimation;

import static com.vastdata.trino.GetTableSizeHelper.getTableSizeEstimate;
import static com.vastdata.trino.VastSessionProperties.getAdaptivePartitioning;
import static com.vastdata.trino.VastSessionProperties.getDataEndpoints;
import static com.vastdata.trino.VastSessionProperties.getDynamicFilterCompactionThreshold;
import static com.vastdata.trino.VastSessionProperties.getDynamicFilterElysiumCompactionMultiplier;
import static com.vastdata.trino.VastSessionProperties.getDynamicFilterPushdownThreshold;
import static com.vastdata.trino.VastSessionProperties.getDynamicFilteringWaitTimeout;
import static com.vastdata.trino.VastSessionProperties.getEstimateSplitsFromElysium;
import static com.vastdata.trino.VastSessionProperties.getEstimateSplitsFromRowIdPredicate;
import static com.vastdata.trino.VastSessionProperties.getNumOfSplits;
import static com.vastdata.trino.VastSessionProperties.getNumOfSubSplits;
import static com.vastdata.trino.VastSessionProperties.getOnlyOrderedPushdown;
import static com.vastdata.trino.VastSessionProperties.getQueryDataRowsPerSplit;
import static com.vastdata.trino.VastSessionProperties.getRowGroupsPerSubSplit;
import static com.vastdata.trino.VastSessionProperties.getSplitSizeMultiplier;
import static com.vastdata.trino.statistics.FilterEstimator.estimateSelectivity;
import static com.vastdata.trino.statistics.FilterEstimator.splitDomains;
import static com.vastdata.trino.statistics.FilterEstimator.splitDomainsLike;
import static com.vastdata.trino.statistics.FilterEstimator.simplifyFilters;
import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class VastSplitManager
        implements ConnectorSplitManager
{
    private static final Logger LOG = Logger.get(VastSplitManager.class);
    private static final ConnectorSplitSource.ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitSource.ConnectorSplitBatch(ImmutableList.of(), false);

    private final VastClient client;
    private final VastStatisticsManager statisticsManager;

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
        return new VastSplitSource(client, statisticsManager, (VastTransaction) transaction, session,
                                   (VastTableHandle) connectorTableHandle, dynamicFilter);
    }

    private static class VastSplitSource
            implements ConnectorSplitSource
    {
        private final VastClient client;
        private final VastStatisticsManager statisticsManager;
        private final VastTransaction tx;
        private final ConnectorSession session;
        private final VastTableHandle table;
        private final DynamicFilter dynamicFilter;
        private final Stopwatch dynamicFilterWaitStopwatch;
        private final long dynamicFilteringWaitTimeoutMillis;
        private final String fullTableName;
        private final VastTraceToken traceToken;
        private final VastRowsEstimator vastRowsEstimator = new VastRowsEstimator();
        private List<ConnectorSplit> splits = null;
        private int offset;

        public VastSplitSource(VastClient client,
                               VastStatisticsManager statisticsManager,
                               VastTransaction tx,
                               ConnectorSession session,
                               VastTableHandle table,
                               DynamicFilter dynamicFilter)
        {
            this.client = client;
            this.statisticsManager = statisticsManager;
            this.tx = tx;
            this.session = session;
            this.table = table;
            this.dynamicFilter = dynamicFilter;
            this.dynamicFilterWaitStopwatch = Stopwatch.createStarted();
            this.dynamicFilteringWaitTimeoutMillis = getDynamicFilteringWaitTimeout(session);
            this.fullTableName = format("%s/%s", table.getSchemaName(), table.getTableName());
            this.traceToken = tx.generateTraceToken(session.getTraceToken());
        }

        private void estimateSplits()
        {
            final String endUser = session.getUser();
            VastSchedulingInfo schedulingInfo = client.getSchedulingInfo(tx, traceToken, table.getSchemaName(), table.getTableName(), endUser);

            TableStatistics ts = statisticsManager.getTableStatistics(table).orElse(TableStatistics.empty());
            Estimate rowsEstimate = ts.getRowCount();
            TupleDomain<VastColumnHandle> tupleDomain = table.getPredicate();
            TupleDomain<VastColumnHandle> dynamicPredicate = dynamicFilter
                .getCurrentPredicate()
                .transformKeys(VastColumnHandle.class::cast);
            List<String> sorted = null;
            if (!getEstimateSplitsFromRowIdPredicate(session) && getEstimateSplitsFromElysium(session)
                && !((tupleDomain.isAll() && dynamicPredicate.isAll()) || (tupleDomain.isNone() && dynamicPredicate.isNone()))) {
                // Get sorted columns from table handle
                Optional<List<String>> sortedColumns = table.getSortedColumns();
                if (sortedColumns.isPresent() && !sortedColumns.orElseThrow().isEmpty()) {
                    sorted = sortedColumns.orElseThrow();
                }
            }

            // If only_ordered_pushdown is enabled, filter dynamic predicate to only include sorted columns
            if (getOnlyOrderedPushdown(session)) {
                // Get sorted columns from table handle
                Optional<List<String>> sortedColumns = table.getSortedColumns();
                if (sortedColumns.isPresent() && !sortedColumns.orElseThrow().isEmpty()) {
                    // Filter dynamic predicate to only include sorted columns
                    final List<String> finalSortedColumns = sortedColumns.orElseThrow(); // Make it final for lambda
                    Map<VastColumnHandle, Domain> dynamicDomains = dynamicPredicate.getDomains().orElse(Map.of());
                    Map<VastColumnHandle, Domain> filteredDomains = dynamicDomains.entrySet().stream()
                        .filter(entry -> finalSortedColumns.contains(entry.getKey().getField().getName()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    dynamicPredicate = TupleDomain.withColumnDomains(filteredDomains);
                    LOG.debug("QueryData(%s) filtered dynamic predicate to sorted columns only: %s", traceToken, dynamicPredicate);
                }
            }
            if (sorted == null || getEstimateSplitsFromRowIdPredicate(session)) {
                dynamicPredicate = dynamicPredicate.simplify(getDynamicFilterCompactionThreshold(session));
                LOG.debug("QueryData(s) compacted dynamic predicate: %s", traceToken, dynamicPredicate);
                dynamicPredicate = simplifyFilters(dynamicPredicate, ts, getDynamicFilterPushdownThreshold(session));
                LOG.debug("QueryData(s) simplified dynamic predicate: %s", traceToken, dynamicPredicate);
                tupleDomain = dynamicPredicate.intersect(tupleDomain);
            }
            TupleDomain<VastColumnHandle> domainForEstimation = tupleDomain;
            if (tupleDomain.isNone() || dynamicPredicate.isNone()) {
                tupleDomain = TupleDomain.none();
                dynamicPredicate = tupleDomain;
                domainForEstimation = tupleDomain;
            }
            else if (getEstimateSplitsFromRowIdPredicate(session)) {
                rowsEstimate = vastRowsEstimator.getMinimalRowsEstimation(table.getPredicate(), rowsEstimate);
                ts = new TableStatistics(rowsEstimate, ts.getColumnStatistics());
            }
            else if (sorted != null) {
                TupleDomain<VastColumnHandle> allPredicates = tupleDomain.intersect(dynamicPredicate);
                TupleDomain<VastColumnHandle>[] sortedAndNotSortedPredicates = splitDomains(allPredicates, sorted);
                TupleDomain<VastColumnHandle>[] splitDynamic = splitDomainsLike(dynamicPredicate, sortedAndNotSortedPredicates[0]);
                TupleDomain<VastColumnHandle>[] splitTuple = splitDomainsLike(tupleDomain, sortedAndNotSortedPredicates[0]);
                TupleDomain<VastColumnHandle> sortedKeyPredicates =
                    splitDynamic[0].simplify(getDynamicFilterElysiumCompactionMultiplier(session) * getDynamicFilterCompactionThreshold(session)).intersect(splitTuple[0]);
                splitDynamic[1] = splitDynamic[1].simplify(getDynamicFilterCompactionThreshold(session));
                LOG.debug("QueryData(%s) compacted sorted key predicates: %s, compacted unsorted key predicates: %s",
                          traceToken, sortedKeyPredicates, splitDynamic[1]);
                sortedKeyPredicates = simplifyFilters(sortedKeyPredicates, ts, getDynamicFilterCompactionThreshold(session));
                splitDynamic[1] = simplifyFilters(splitDynamic[1], ts, getDynamicFilterCompactionThreshold(session));
                LOG.debug("QueryData(%s) simplified sorted key predicates: %s, simplified unsorted key predicates: %s",
                          traceToken, sortedKeyPredicates, splitDynamic[1]);
                tupleDomain = TupleDomain.intersect(List.of(sortedKeyPredicates, splitDynamic[1], splitTuple[1]));
                domainForEstimation = tupleDomain;
                if (!sortedKeyPredicates.isAll() || rowsEstimate.isUnknown()) {
                    try {
                        long estimate = getTableSizeEstimate(table, sortedKeyPredicates,
                                                             tx, traceToken, schedulingInfo, client, session);
                        LOG.debug("QueryData(%s) estimateSplits got estimate from Vast: %s (%s)", traceToken, estimate, rowsEstimate.toString());
                        if (rowsEstimate.isUnknown() || rowsEstimate.getValue() > estimate) {
                            LOG.debug("QueryData(%s) estimateSplits updating the estimate", traceToken);
                            domainForEstimation = splitDynamic[1].intersect(splitTuple[1]); // non-sorted column predicate
                            rowsEstimate = Estimate.of(estimate);
                            ts = new TableStatistics(rowsEstimate, ts.getColumnStatistics());
                        }
                    }
                    catch (Exception e) {
                        LOG.error(e, "Failed to estimate sorted columns splits optimization: %s", traceToken);
                    }
                }
            }
            int numOfSplits = estimateNumOfSplits(traceToken, session, domainForEstimation, ts, table.getLimit());
            LOG.debug("QueryData(%s) estimateSplits using %d splits for %s", traceToken, numOfSplits, fullTableName);
            int numOfSubSplits = getNumOfSubSplits(session);
            int rowGroupsPerSubSplit = getRowGroupsPerSubSplit(session);
            final TupleDomain<VastColumnHandle> finalDomain = tupleDomain;
            splits = IntStream
                .range(0, numOfSplits)
                .mapToObj(currentSplit -> {
                        VastSplitContext context = new VastSplitContext(currentSplit, numOfSplits, numOfSubSplits, rowGroupsPerSubSplit);
                        return new VastSplit(getDataEndpoints(session), context, schedulingInfo, finalDomain);
                    })
                .collect(Collectors.toList());
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
        {
            if (splits == null) {
                long timeLeft = dynamicFilteringWaitTimeoutMillis - dynamicFilterWaitStopwatch.elapsed(TimeUnit.MILLISECONDS);
                if (dynamicFilter.isAwaitable() && timeLeft > 0) {
                    LOG.debug("QueryData(%s) getNextBatch blocking %s split generation for %.3f seconds", traceToken, fullTableName, timeLeft / 1e3);
                    return dynamicFilter.isBlocked()
                            .thenApply(ignored -> EMPTY_BATCH)
                            .completeOnTimeout(EMPTY_BATCH, timeLeft, TimeUnit.MILLISECONDS);
                }
                LOG.debug("QueryData(%s) getNextBatch estimating splits for %s, Dynamic filter still awaitable: %s", traceToken, maxSize, fullTableName, dynamicFilter.isAwaitable());
                estimateSplits();
            }

            int remainingSplits = splits.size() - offset;
            int size = Math.min(remainingSplits, maxSize);
            List<ConnectorSplit> results = splits.subList(offset, offset + size);
            offset += size;
            LOG.debug("QueryData(%s) getNextBatch returning %s splits", traceToken, size);
            return completedFuture(new ConnectorSplitBatch(results, isFinished()));
        }

        @Override
        public void close()
        {
            LOG.debug("QueryData(%s) close()", traceToken);
        }

        @Override
        public boolean isFinished()
        {
            return splits == null || offset >= splits.size();
        }
    }

    private static int estimateNumOfSplits(VastTraceToken traceToken, ConnectorSession session, TupleDomain<VastColumnHandle> tupleDomain,
                                           TableStatistics statistics, Optional<Long> limit)
    {
        LOG.debug("QueryData(%s) estimateNumOfSplits split planning parameters: %d, %b, %d, %d", traceToken, getNumOfSplits(session), getAdaptivePartitioning(session),
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
        Supplier<Optional<Double>> rowsEstimateSupplier = () -> statistics.getRowCount().isUnknown() ?
                getRowsFromLimit(tupleDomain, limit) :
                getRowsFromStats(statistics);
        LOG.debug("QueryData(%s) estimateNumOfSplits row estimate: %s", traceToken, rowsEstimateSupplier.get());
        return getNumOfSplitsEstimation(useMultiplier, selectivityEstimation, maxSplitsSupplier,
                                        rowsPerSplitConf, multiplierConf, rowsEstimateSupplier);
    }

    private static Optional<Double> getRowsFromStats(TableStatistics statistics)
    {
        return Optional.of(statistics.getRowCount().getValue());
    }

    private static Optional<Double> getRowsFromLimit(TupleDomain<VastColumnHandle> tupleDomain, Optional<Long> limit)
    {
        return tupleDomain.isAll() && limit.isPresent() ? optionalLongToOptionalDouble(limit) : Optional.empty();
    }

    private static Optional<Double> optionalLongToOptionalDouble(Optional<Long> limit)
    {
        return Optional.of(limit.orElseThrow().doubleValue());
    }

}
