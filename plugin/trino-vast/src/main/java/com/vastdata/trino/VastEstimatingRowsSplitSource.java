/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.trino.metrics.SplitSourceMetrics;
import com.vastdata.trino.statistics.VastStatisticsManager;
import io.airlift.log.Logger;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.vastdata.client.util.NumOfSplitsEstimator.getNumOfSplitsEstimation;
import static com.vastdata.trino.GetTableSizeHelper.getTableSizeEstimate;
import static com.vastdata.trino.VastSessionProperties.getAdaptivePartitioning;
import static com.vastdata.trino.VastSessionProperties.getDynamicFilterCompactionThreshold;
import static com.vastdata.trino.VastSessionProperties.getDynamicFilterElysiumCompactionMultiplier;
import static com.vastdata.trino.VastSessionProperties.getDynamicFilterPushdownThreshold;
import static com.vastdata.trino.VastSessionProperties.getEstimateSplitsFromElysium;
import static com.vastdata.trino.VastSessionProperties.getEstimateSplitsFromRowIdPredicate;
import static com.vastdata.trino.VastSessionProperties.getNumOfSplits;
import static com.vastdata.trino.VastSessionProperties.getNumOfSubSplits;
import static com.vastdata.trino.VastSessionProperties.getOnlyOrderedPushdown;
import static com.vastdata.trino.VastSessionProperties.getQueryDataRowsPerPage;
import static com.vastdata.trino.VastSessionProperties.getQueryDataRowsPerSplit;
import static com.vastdata.trino.VastSessionProperties.getRowGroupsPerSubSplit;
import static com.vastdata.trino.statistics.FilterEstimator.estimateSelectivity;
import static com.vastdata.trino.statistics.FilterEstimator.simplifyFilters;
import static com.vastdata.trino.statistics.FilterEstimator.splitDomains;
import static com.vastdata.trino.statistics.FilterEstimator.splitDomainsLike;

class VastEstimatingRowsSplitSource
        extends VastSplitSource
{
    private static final Logger LOG = Logger.get(
            VastEstimatingRowsSplitSource.class);

    private final VastRowsEstimator vastRowsEstimator = new VastRowsEstimator();

    public VastEstimatingRowsSplitSource(NodeManager nodeManager,
                                         VastPageSourceProvider vastPageSourceProvider,
                                         VastClient client,
                                         VastStatisticsManager statisticsManager,
                                         SplitSourceMetrics splitSourceMetrics,
                                         VastTransaction tx,
                                         ConnectorSession session,
                                         VastTableHandle table,
                                         DynamicFilter dynamicFilter)
    {
        super(nodeManager, vastPageSourceProvider, statisticsManager, client,
                splitSourceMetrics, tx, session, table, dynamicFilter);
    }

    static int[] estimateNumOfSplits(OptionalLong rowCount,
                                     ConnectorSession session,
                                     TupleDomain<VastColumnHandle> tupleDomain,
                                     int numOfSplits,
                                     int numOfSubSplits,
                                     TableStatistics statistics,
                                     Optional<Long> limit)
    {
        OptionalLong rowEstimate = rowCount.isEmpty() ? getRowsFromLimit(tupleDomain, limit) : rowCount;
        final double selectivityEstimation = estimateSelectivity(tupleDomain, statistics, limit);
        return getNumOfSplitsEstimation(rowEstimate, numOfSplits, numOfSubSplits, getRowGroupsPerSubSplit(session),
                                        getQueryDataRowsPerPage(session), selectivityEstimation,
                                        getQueryDataRowsPerSplit(session), getAdaptivePartitioning(session));
    }

    private static OptionalLong getRowsFromStats(TableStatistics statistics)
    {
        return OptionalLong.of((long) statistics.getRowCount().getValue());
    }

    private static OptionalLong getRowsFromLimit(TupleDomain<VastColumnHandle> tupleDomain,
                                                 Optional<Long> limit)
    {
        return tupleDomain.isAll() && limit.isPresent() ?
                OptionalLong.of(limit.orElseThrow()) :
                OptionalLong.empty();
    }

    protected void createSplits()
    {
        final String endUser = session.getUser();
        TableStatistics ts = statisticsManager
                .getTableStatistics(tableHandle)
                .orElse(TableStatistics.empty());
        Estimate rowsEstimate = ts.getRowCount();
        TupleDomain<VastColumnHandle> tupleDomain = tableHandle.getPredicate();
        TupleDomain<VastColumnHandle> dynamicPredicate = dynamicFilter
                .getCurrentPredicate()
                .transformKeys(VastColumnHandle.class::cast);
        List<String> sorted = null;
        if (!getEstimateSplitsFromRowIdPredicate(
                session) && getEstimateSplitsFromElysium(
                session) && !((tupleDomain.isAll() && dynamicPredicate.isAll()) || (tupleDomain.isNone() && dynamicPredicate.isNone()))) {
            Optional<List<String>> s = tableHandle.getSortedColumns();
            if (s.isPresent() && !s.orElseThrow().isEmpty()) {
                sorted = s.orElseThrow();
            }
        }
        // If only_ordered_pushdown is enabled, filter dynamic predicate to only include sorted columns
        if (getOnlyOrderedPushdown(session)) {
            // Get sorted columns from table handle
            Optional<List<String>> sortedColumns = tableHandle.getSortedColumns();
            if (sortedColumns.isPresent() && !sortedColumns
                    .orElseThrow()
                    .isEmpty()) {
                // Filter dynamic predicate to only include sorted columns
                final List<String> finalSortedColumns = sortedColumns.orElseThrow(); // Make it final for lambda
                Map<VastColumnHandle, Domain> dynamicDomains = dynamicPredicate
                        .getDomains()
                        .orElse(Map.of());
                Map<VastColumnHandle, Domain> filteredDomains = dynamicDomains
                        .entrySet()
                        .stream()
                        .filter(entry -> finalSortedColumns.contains(
                                entry.getKey().getField().getName()))
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                Map.Entry::getValue));
                dynamicPredicate = TupleDomain.withColumnDomains(
                        filteredDomains);
                LOG.debug(
                        "QueryData(%s) filtered dynamic predicate to sorted columns only: %s",
                        traceToken, dynamicPredicate);
            }
        }
        if (sorted == null || getEstimateSplitsFromRowIdPredicate(session)) {
            dynamicPredicate = dynamicPredicate.simplify(
                    getDynamicFilterCompactionThreshold(session));
            LOG.debug("QueryData(s) compacted dynamic predicate: %s",
                    traceToken, dynamicPredicate);
            dynamicPredicate = simplifyFilters(dynamicPredicate, ts,
                    getDynamicFilterPushdownThreshold(session));
            LOG.debug("QueryData(s) simplified dynamic predicate: %s",
                    traceToken, dynamicPredicate);
            tupleDomain = dynamicPredicate.intersect(tupleDomain);
        }
        TupleDomain<VastColumnHandle> domainForEstimation = tupleDomain;
        if (tupleDomain.isNone() || dynamicPredicate.isNone()) {
            tupleDomain = TupleDomain.none();
            domainForEstimation = tupleDomain;
        }
        else if (getEstimateSplitsFromRowIdPredicate(session)) {
            rowsEstimate = vastRowsEstimator.getMinimalRowsEstimation(
                    tableHandle.getPredicate(), rowsEstimate);
            ts = new TableStatistics(rowsEstimate, ts.getColumnStatistics());
        }
        else if (sorted != null) {
            TupleDomain<VastColumnHandle> allPredicates = tupleDomain.intersect(
                    dynamicPredicate);
            TupleDomain<VastColumnHandle>[] sortedAndNotSortedPredicates = splitDomains(
                    allPredicates, sorted);
            TupleDomain<VastColumnHandle>[] splitDynamic = splitDomainsLike(
                    dynamicPredicate, sortedAndNotSortedPredicates[0]);
            TupleDomain<VastColumnHandle>[] splitTuple = splitDomainsLike(
                    tupleDomain, sortedAndNotSortedPredicates[0]);
            TupleDomain<VastColumnHandle> sortedKeyPredicates = splitDynamic[0]
                    .simplify(getDynamicFilterElysiumCompactionMultiplier(
                            session) * getDynamicFilterCompactionThreshold(
                            session))
                    .intersect(splitTuple[0]);
            splitDynamic[1] = splitDynamic[1].simplify(
                    getDynamicFilterCompactionThreshold(session));
            LOG.debug(
                    "QueryData(%s) compacted sorted key predicates: %s, compacted unsorted key predicates: %s",
                    traceToken, sortedKeyPredicates, splitDynamic[1]);
            sortedKeyPredicates = simplifyFilters(sortedKeyPredicates, ts,
                    getDynamicFilterCompactionThreshold(session));
            splitDynamic[1] = simplifyFilters(splitDynamic[1], ts,
                    getDynamicFilterCompactionThreshold(session));
            LOG.debug(
                    "QueryData(%s) simplified sorted key predicates: %s, simplified unsorted key predicates: %s",
                    traceToken, sortedKeyPredicates, splitDynamic[1]);
            tupleDomain = TupleDomain.intersect(
                    List.of(sortedKeyPredicates, splitDynamic[1],
                            splitTuple[1]));
            domainForEstimation = tupleDomain;
            if (!sortedKeyPredicates.isAll() || rowsEstimate.isUnknown()) {
                try {
                    long estimate = getTableSizeEstimate(vastPageSourceProvider,
                            tableHandle, sortedKeyPredicates, tx, traceToken,
                            session);
                    LOG.debug(
                            "QueryData(%s) estimateSplits got estimate from Vast: %s (%s)",
                            traceToken, estimate, rowsEstimate.toString());
                    if (rowsEstimate.isUnknown() || rowsEstimate.getValue() > estimate) {
                        LOG.debug(
                                "QueryData(%s) estimateSplits updating the estimate",
                                traceToken);
                        domainForEstimation = splitDynamic[1].intersect(
                                splitTuple[1]); // non-sorted column predicate
                        rowsEstimate = Estimate.of(estimate);
                        ts = new TableStatistics(rowsEstimate,
                                ts.getColumnStatistics());
                    }
                }
                catch (Exception e) {
                    LOG.error(e,
                            "Failed to estimate sorted columns splits optimization: %s",
                            traceToken);
                }
            }
        }
        OptionalLong rowEstimate = ts.getRowCount().isUnknown() ?
                OptionalLong.empty() :
                getRowsFromStats(ts);

        VastSchedulingInfo schedulingInfo = client.getSchedulingInfo(tx,
                traceToken, tableHandle.getSchemaName(),
                tableHandle.getTableName(), endUser);
        int numOfSubSplits = getNumOfSubSplits(session);
        if (tupleDomain.isAll()) {
            numOfSubSplits = 1; // in full scan we don't need sub-splits
        }

        int[] numOfSplits = estimateNumOfSplits(rowEstimate, session,
                domainForEstimation, getNumOfSplits(session), numOfSubSplits, ts, tableHandle.getLimit());
        LOG.debug("QueryData(%s) estimateSplits using %s splits for %s",
                traceToken, Arrays.toString(numOfSplits), fullTableName);
        int rowGroupsPerSubSplit = getRowGroupsPerSubSplit(session);
        final TupleDomain<VastColumnHandle> finalDomain = tupleDomain;
        List<HostAddress> workerAddresses = nodeManager
                .getWorkerNodes()
                .stream()
                .map(Node::getHostAndPort)
                .toList();

        List<ConnectorSplit> splits = createSplits(numOfSplits,
                rowGroupsPerSubSplit, workerAddresses, schedulingInfo,
                finalDomain);
        splitsQueue.addAll(splits);
    }

    @Override
    public boolean isSplitSourceFinished()
    {
        return initialSplitsCollected && splitsQueue.isEmpty();
    }
}
