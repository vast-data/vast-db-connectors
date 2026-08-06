/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.VastSplitContext;
import com.vastdata.client.error.VastException;
import com.vastdata.client.partition.PartitionColumnMetadata;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.trino.metrics.SplitSourceMetrics;
import com.vastdata.trino.statistics.VastStatisticsManager;
import io.airlift.log.Logger;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.HostAddress;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.TableStatistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.vastdata.client.util.NumOfSplitsEstimator.SPLITS_IDX;
import static com.vastdata.client.util.NumOfSplitsEstimator.SUBSPLITS_IDX;
import static com.vastdata.trino.VastSessionProperties.getDataEndpoints;
import static com.vastdata.trino.VastSessionProperties.getDynamicFilterCompactionThreshold;
import static com.vastdata.trino.VastSessionProperties.getDynamicFilteringWaitTimeout;
import static java.lang.String.format;

public abstract class VastSplitSource
        implements ConnectorSplitSource
{
    static final int MAX_DF_RETRY_COUNT = 5; // To avoid retrying too many times when dynamic filter is not being produced, we set a max retry count. After reaching the max retry count, we will stop waiting for dynamic filter and return the splits immediately.
    private static final Logger LOG = Logger.get(VastSplitSource.class);
    private static final ConnectorSplitSource.ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitSource.ConnectorSplitBatch(
            ImmutableList.of(), false);
    protected final NodeManager nodeManager;
    protected final VastPageSourceProvider vastPageSourceProvider;
    protected final VastStatisticsManager statisticsManager;
    protected final SplitSourceMetrics globalSplitSourceMetrics;
    protected final SplitSourceMetrics querySplitSourceMetrics;
    protected final String fullTableName;
    protected final VastTraceToken traceToken;
    protected final DynamicFilter dynamicFilter;
    protected final VastClient client;
    protected final VastTransaction tx;
    protected final ConnectorSession session;
    protected final VastTableHandle tableHandle;
    private final int dynamicFilteringWaitTimeoutMillis;
    private final DynamicFilterContext dynamicFilterContext;
    protected BlockingQueue<ConnectorSplit> splitsQueue;
    protected boolean initialSplitsCollected;

    public VastSplitSource(NodeManager nodeManager,
                           VastPageSourceProvider vastPageSourceProvider,
                           VastStatisticsManager statisticsManager,
                           VastClient client,
                           SplitSourceMetrics globalSplitSourceMetrics,
                           VastTransaction tx,
                           ConnectorSession session,
                           VastTableHandle tableHandle,
                           DynamicFilter dynamicFilter)
    {
        this.nodeManager = nodeManager;
        this.vastPageSourceProvider = vastPageSourceProvider;
        this.statisticsManager = statisticsManager;
        this.client = client;
        this.tx = tx;
        this.session = session;
        this.tableHandle = tableHandle;
        this.dynamicFilteringWaitTimeoutMillis = getDynamicFilteringWaitTimeout(
                session);
        this.fullTableName = format("%s/%s", tableHandle.getSchemaName(),
                tableHandle.getTableName());
        this.traceToken = tx.generateTraceToken(session.getTraceToken());
        this.dynamicFilter = dynamicFilter;
        this.dynamicFilterContext = buildDynamicFilterContext();
        this.splitsQueue = new LinkedBlockingQueue<>();
        this.globalSplitSourceMetrics = globalSplitSourceMetrics;
        this.querySplitSourceMetrics = new SplitSourceMetrics();
    }

    @Override
    public void close()
    {
    }

    @VisibleForTesting
    DynamicFilterContext getDynamicFilterContext()
    {
        return dynamicFilterContext;
    }

    private DynamicFilterContext buildDynamicFilterContext()
    {
        long waitTime;
        if (!dynamicFilter.isAwaitable()) {
            return new DynamicFilterContext(dynamicFilter, Optional.empty(), 0);
        }
        Set<String> dfColumnsName = dynamicFilter
                .getColumnsCovered()
                .stream()
                .map(ch -> (VastColumnHandle) ch)
                .map(ch -> ch.getField().getName())
                .collect(Collectors.toSet());
        if (dfColumnsName.isEmpty()) {
            return new DynamicFilterContext(dynamicFilter, Optional.of(true),
                    dynamicFilteringWaitTimeoutMillis);
        }
        // Check any of the dynamic filter columns is sorted column
        Optional<String> sortedColumn = tableHandle
                .getSortedColumns()
                .orElse(Collections.emptyList())
                .stream()
                .filter(dfColumnsName::contains)
                .findFirst();
        if (sortedColumn.isPresent()) {
            LOG.info(
                    "QueryData(%s) calculateWaitTimeMillis table %s/%s DF on sorted column %s",
                    traceToken, tableHandle.getSchemaName(),
                    tableHandle.getTableName(), sortedColumn);
            return new DynamicFilterContext(dynamicFilter, Optional.of(false),
                    (long) dynamicFilteringWaitTimeoutMillis * VastSessionProperties.getDynamicFilteringWaitTimeoutFactor(
                            session));
        }
        // Check any of the dynamic filter columns is partition column
        Optional<PartitionColumnMetadata> partitionColumn = tableHandle
                .getPartitionColumns()
                .orElse(Collections.emptyList())
                .stream()
                .filter(pcm -> dfColumnsName.contains(pcm.getColumnName()))
                .findFirst();
        if (partitionColumn.isPresent()) {
            LOG.info(
                    "QueryData(%s) calculateWaitTimeMillis table %s/%s DF on partition column %s",
                    traceToken, tableHandle.getSchemaName(),
                    tableHandle.getTableName(), partitionColumn);
            return new DynamicFilterContext(dynamicFilter, Optional.of(false),
                    (long) dynamicFilteringWaitTimeoutMillis * VastSessionProperties.getDynamicFilteringWaitTimeoutFactor(
                            session));
        }
        OptionalDouble tableStatMaxWaitOpt = OptionalDouble.empty();
        Optional<TableStatistics> tableStatisticsOpt = statisticsManager.getTableStatistics(
                tableHandle);
        if (tableStatisticsOpt.isPresent()) {
            TableStatistics tableStatistics = tableStatisticsOpt.orElseThrow();
            if (!tableStatistics.getRowCount().isUnknown()) {
                tableStatMaxWaitOpt = dynamicFilter
                        .getColumnsCovered()
                        .stream()
                        .mapToDouble(vch ->
                        {
                            ColumnStatistics columnStatistics = tableStatistics
                                    .getColumnStatistics()
                                    .get(vch);
                            if (columnStatistics == null || columnStatistics
                                    .getDistinctValuesCount()
                                    .isUnknown()) {
                                return dynamicFilteringWaitTimeoutMillis;
                            }
                            double distinctValuesCount = columnStatistics
                                    .getDistinctValuesCount()
                                    .getValue();
                            // If the distinct values count is high compared to the table row count (i.e. the column is highly selective),
                            // we can expect that dynamic filtering will be effective, so we can wait longer.
                            if (tableStatistics
                                    .getRowCount()
                                    .getValue() > 0 && (tableStatistics
                                    .getRowCount()
                                    .getValue() / distinctValuesCount) < getDynamicFilterCompactionThreshold(
                                    session)) {
                                return dynamicFilteringWaitTimeoutMillis * (tableStatistics
                                        .getRowCount()
                                        .getValue() / distinctValuesCount);
                            }
                            else {
                                return dynamicFilteringWaitTimeoutMillis;
                            }
                        })
                        .max();
            }
        }
        waitTime = (long) Math.max(
                tableStatMaxWaitOpt.orElse(dynamicFilteringWaitTimeoutMillis),
                dynamicFilteringWaitTimeoutMillis);
        return new DynamicFilterContext(dynamicFilter, Optional.of(true),
                waitTime);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        if (!initialSplitsCollected) {
            if (dynamicFilterContext.shouldWait()) {
                long waitTime = dynamicFilterContext.waitTime;
                LOG.info(
                        "QueryData(%s) getNextBatch table %s/%s estimating DF wait time %s, DFRetryCount %d",
                        traceToken, tableHandle.getSchemaName(),
                        tableHandle.getTableName(), waitTime,
                        dynamicFilterContext.retryCount);
                dynamicFilterContext.incrementRetryCount();
                return dynamicFilter
                        .isBlocked()
                        .thenApply(ignored -> EMPTY_BATCH)
                        .completeOnTimeout(EMPTY_BATCH, waitTime,
                                TimeUnit.MILLISECONDS);
            }
            LOG.debug(
                    "QueryData(%s) getNextBatch table %s estimating splits for %s, Dynamic filter still awaitable? %s, DFRetryCount: %d",
                    traceToken, maxSize, fullTableName,
                    dynamicFilter.isAwaitable(),
                    dynamicFilterContext.retryCount);

            try {
                createSplits();
            }
            catch (VastException e) {
                CompletableFuture<ConnectorSplitBatch> failedFuture = new CompletableFuture<>();
                failedFuture.completeExceptionally(e);
                return failedFuture;
            }
            initialSplitsCollected = true;
        }
        int batchSize = Math.min(maxSize, Runtime.getRuntime().availableProcessors() * nodeManager.getWorkerNodes().size());
        List<ConnectorSplit> results = new ArrayList<>(batchSize);
        splitsQueue.drainTo(results, batchSize);
        return CompletableFuture.completedFuture(
                new ConnectorSplitBatch(results, isFinished()));
    }

    protected abstract void createSplits()
            throws VastException;

    protected abstract boolean isSplitSourceFinished();

    @Override
    public boolean isFinished()
    {
        boolean isSplitSourceFinished = isSplitSourceFinished();
        if (isSplitSourceFinished) {
            globalSplitSourceMetrics.merge(querySplitSourceMetrics);
        }
        return isSplitSourceFinished;
    }

    @Override
    public Metrics getMetrics()
    {
        Map<String, LongCount> diffMetrics = querySplitSourceMetrics
                .diffMetrics()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> new LongCount(e.getValue())));
        Map<String, Metric<?>> allMetrics = new HashMap<>(diffMetrics);
        return new Metrics(allMetrics);
    }

    protected List<ConnectorSplit> createSplits(int[] numOfSplits,
                                                int rowGroupsPerSubSplit,
                                                List<HostAddress> workerAddresses,
                                                VastSchedulingInfo schedulingInfo,
                                                TupleDomain<VastColumnHandle> finalDomain)
    {
        return IntStream
                .range(0, numOfSplits[SPLITS_IDX])
                .mapToObj(currentSplit ->
                {
                    VastSplitContext context = new VastSplitContext(
                            currentSplit, numOfSplits[SPLITS_IDX],
                            numOfSplits[SUBSPLITS_IDX], rowGroupsPerSubSplit);
                    final HostAddress assignedNodeAddress = workerAddresses.get(
                            currentSplit % workerAddresses.size());
                    return new VastSplit(assignedNodeAddress,
                            getDataEndpoints(session), context, schedulingInfo,
                            finalDomain, traceToken.toString());
                })
                .collect(Collectors.toList());
    }

    static class DynamicFilterContext
    {
        final DynamicFilter dynamicFilter;
        final Optional<Boolean> waitOnce; // empty = no wait, true = waitOnce, false = wait until MAX_RETRY
        final long waitTime;
        int retryCount;

        private DynamicFilterContext(DynamicFilter dynamicFilter,
                                     Optional<Boolean> waitOnce,
                                     long waitTime)
        {
            this.dynamicFilter = dynamicFilter;
            this.waitOnce = waitOnce;
            this.waitTime = waitTime;
        }

        public boolean shouldWait()
        {
            if (waitOnce.isEmpty()) {
                return false;
            }
            if (waitOnce.orElseThrow()) {
                return dynamicFilter.isAwaitable() && retryCount == 0;
            }
            return dynamicFilter.isAwaitable() && !dynamicFilter.isComplete() && (retryCount < MAX_DF_RETRY_COUNT);
        }

        public void incrementRetryCount()
        {
            retryCount++;
        }
    }
}
