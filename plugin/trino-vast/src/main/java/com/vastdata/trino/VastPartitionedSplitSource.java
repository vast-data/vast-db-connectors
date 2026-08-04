/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.VastSplitContext;
import com.vastdata.client.error.VastException;
import com.vastdata.client.partition.PartitionColumnMetadata;
import com.vastdata.client.tx.VastAutocommitTransaction;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.client.tx.VastTransactionHandleManager;
import com.vastdata.trino.metrics.SplitSourceMetrics;
import com.vastdata.trino.statistics.VastStatisticsManager;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.airlift.log.Logger;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.partition.PartitionConstants.PIT_METADATA_COLUMN_NAMES;
import static com.vastdata.client.partition.PartitionConstants.PIT_NAME_SUFFIX;
import static com.vastdata.client.util.NumOfSplitsEstimator.SPLITS_IDX;
import static com.vastdata.client.util.NumOfSplitsEstimator.SUBSPLITS_IDX;
import static com.vastdata.trino.VastEstimatingRowsSplitSource.estimateNumOfSplits;
import static com.vastdata.trino.VastSessionProperties.getDataEndpoints;
import static com.vastdata.trino.VastSessionProperties.getDynamicFilterElysiumCompactionMultiplier;
import static com.vastdata.trino.VastSessionProperties.getDynamicFilterPushdownThreshold;
import static com.vastdata.trino.VastSessionProperties.getMinRowsForPartitionSplitEstimation;
import static com.vastdata.trino.VastSessionProperties.getNumOfSplits;
import static com.vastdata.trino.VastSessionProperties.getNumOfSubSplits;
import static com.vastdata.trino.VastSessionProperties.getRowGroupsPerSubSplit;
import static com.vastdata.trino.statistics.FilterEstimator.simplifyFilters;
import static java.util.Objects.requireNonNull;

public class VastPartitionedSplitSource
        extends VastSplitSource
{
    private static final Logger LOG = Logger.get(
            VastPartitionedSplitSource.class);
    private static final Field ROW_ESTIMATE_FIELD = new Field("est_row_count",
            FieldType.nullable(new ArrowType.Int(64, true)), List.of());
    static final Type TRINO_ROW_ESTIMATE_TRINO_TYPE = TypeUtils.convertArrowFieldToTrinoType(
            ROW_ESTIMATE_FIELD);
    private final VastTransactionHandleManager<VastTransactionHandle> vastTransactionHandleManager;
    private final Set<PartitionSplitsLoader> pendingPartitionPreEstimation;
    private final ExecutorService splitLoaderExecutor;

    public VastPartitionedSplitSource(NodeManager nodeManager,
                                      VastClient vastClient,
                                      VastStatisticsManager statisticsManager,
                                      VastPageSourceProvider vastPageSourceProvider,
                                      VastTransactionHandleManager<VastTransactionHandle> vastTransactionHandleManager,
                                      SplitSourceMetrics globalSplitSourceMetrics,
                                      VastTransaction vastTransaction,
                                      ConnectorSession session,
                                      VastTableHandle vastTableHandle,
                                      DynamicFilter dynamicFilter)
    {
        super(nodeManager, vastPageSourceProvider, statisticsManager,
                vastClient, globalSplitSourceMetrics, vastTransaction, session,
                vastTableHandle, dynamicFilter);
        this.vastTransactionHandleManager = requireNonNull(
                vastTransactionHandleManager);
        this.pendingPartitionPreEstimation = new HashSet<>();
        this.splitLoaderExecutor = Executors.newFixedThreadPool(10);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        return super.getNextBatch(maxSize).thenCompose(batch ->
        {
            synchronized (pendingPartitionPreEstimation) {
                if (pendingPartitionPreEstimation.isEmpty()) {
                    return CompletableFuture.completedFuture(batch);
                }
                List<CompletableFuture<Void>> futures = pendingPartitionPreEstimation
                        .stream()
                        .map(PartitionSplitsLoader::getCompletionFuture)
                        .toList();
                // Fail if ANY future fails, complete when ANY succeeds
                CompletableFuture<Object> result = CompletableFuture.anyOf(
                        futures.toArray(new CompletableFuture[0]));
                futures.forEach(f -> f.exceptionally(ex ->
                {
                    result.completeExceptionally(ex);
                    return null;
                }));
                return result.thenApply(ignored -> batch);
            }
        });
    }

    @Override
    protected void createSplits()
            throws VastException
    {
        List<Field> tableFields = tableHandle
                .getColumnHandlesCache()
                .stream()
                .map(VastColumnHandle::getBaseField)
                .toList();
        String partitionTableName = tableHandle.getTableName() + PIT_NAME_SUFFIX;
        try (VastAutocommitTransaction partitionTx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                vastTransactionHandleManager,
                () -> vastTransactionHandleManager.startTransaction(
                        session.getUser()), session.getUser())) {
            VastSplitContext context = new VastSplitContext(0, 1, 1, 1);
            List<Field> pitFields = client.listColumns(partitionTx, tableHandle.getSchemaName(),
                            partitionTableName, 1000, tableHandle.getExtraQueryParams(), session.getUser()).getFields();
            List<PartitionMetadata> projectedPartitionValueColumns = getPartitionTableColumns(tableFields, pitFields);
            TupleDomain<VastColumnHandle> partitionedPredicates = convertQueryPredicates(tableFields, projectedPartitionValueColumns);
            List<VastSubstringMatch> partitionSubstringMatchers = convertQuerySubstringMatchers(projectedPartitionValueColumns);
            VastTableHandle partitionTableHandle = new VastTableHandle(
                    tableHandle.getSchemaName(), partitionTableName, List.of(),
                    Optional.empty(), Optional.empty(), partitionedPredicates, null,
                    Optional.empty(), partitionSubstringMatchers, Optional.empty(),
                    Optional.empty(), tableHandle.getExtraQueryParams(), false, false,
                    tableHandle.getHandleID());
            LOG.debug("getting partition splits for table %s with predicates: %s",
                    partitionTableName, partitionedPredicates);
            List<URI> dataEndpoints = getDataEndpoints(session);
            VastSplit partitionQuerySplit = new VastSplit(null, dataEndpoints,
                    context, null, partitionedPredicates,
                    traceToken.toString());
            VastSchedulingInfo schedulingInfo = client.getSchedulingInfo(partitionTx, traceToken, tableHandle.getSchemaName(),
                    tableHandle.getTableName(), session.getUser());
            List<ConnectorSplit> splits = createSplits(partitionTx,
                    partitionQuerySplit, partitionTableHandle,
                    projectedPartitionValueColumns, dataEndpoints,
                    schedulingInfo);
            LOG.debug("calculated %d partition splits for table %s",
                    splits.size(), partitionTableName);
            splitsQueue.addAll(splits);
            synchronized (pendingPartitionPreEstimation) {
                pendingPartitionPreEstimation.forEach(
                        splitLoaderExecutor::submit);
            }
        }
        catch (IOException e) {
            LOG.warn(e, "failed to read partition splits for table %s", partitionTableName);
            throw toRuntime(e);
        }
    }

    private List<ConnectorSplit> createSplits(VastAutocommitTransaction partitionTx,
                                              VastSplit partitionQuerySplit,
                                              VastTableHandle partitionTableHandle,
                                              List<PartitionMetadata> partitionValueColumns,
                                              List<URI> dataEndpoints,
                                              VastSchedulingInfo schedulingInfo)
            throws IOException
    {
        List<ColumnHandle> partitionColumnHandles = new ArrayList<>();
        partitionValueColumns
                .stream()
                .map(md -> md.isIdentity() ? md.tableColumnHandle : md.partitionFilterColumnHandle)
                .forEach(partitionColumnHandles::add);
        partitionColumnHandles.add(
                VastColumnHandle.fromField(ROW_ESTIMATE_FIELD));
        int estimateColIndex = partitionColumnHandles.size() - 1;
        TupleDomain<VastColumnHandle> dynamicPredicate = dynamicFilter
                .getCurrentPredicate()
                .transformKeys(VastColumnHandle.class::cast);

        List<ConnectorSplit> ret = new ArrayList<>();
        List<HostAddress> workerAddresses = nodeManager
                .getWorkerNodes()
                .stream()
                .map(Node::getHostAndPort)
                .toList();

        try (ConnectorPageSource vastPageSource = vastPageSourceProvider.createPageSource(
                (VastTransactionHandle) partitionTx.getTransaction(), session,
                partitionQuerySplit, partitionTableHandle,
                partitionColumnHandles, DynamicFilter.EMPTY)) {
            TableStatistics tableTS = statisticsManager
                    .getTableStatistics(tableHandle)
                    .orElse(TableStatistics.empty());
            int rowGroupsPerSubSplit = getRowGroupsPerSubSplit(session);
            while (!vastPageSource.isFinished()) {
                SourcePage page = vastPageSource.getNextSourcePage();
                if (page != null) {
                    int rowCount = page.getPositionCount();
                    for (int row = 0; row < rowCount; row++) {
                        Map<VastColumnHandle, Domain> domains = new HashMap<>();
                        for (int col = 0; col < partitionValueColumns.size(); col++) {
                            VastColumnHandle columnHandle = partitionValueColumns
                                    .get(col)
                                    .splitFilterColumnHandle();
                            if (page.getBlock(col).isNull(row)) {
                                domains.put(columnHandle, Domain.onlyNull(
                                        columnHandle
                                                .getColumnMetadata()
                                                .getType()));
                            }
                            else {
                                Type fieldType = TypeUtils.convertArrowFieldToTrinoType(
                                        partitionValueColumns.get(
                                                col).partitionFilterColumnHandle.getField());
                                Object partitionValue = TypeUtils.getRowValue(
                                        fieldType, page.getBlock(col), row);
                                domains.put(columnHandle, Domain.singleValue(
                                        columnHandle
                                                .getColumnMetadata()
                                                .getType(), partitionValue));
                            }
                        }
                        TupleDomain<VastColumnHandle> partitionFilters = TupleDomain.withColumnDomains(
                                domains);
                        dynamicPredicate = dynamicPredicate.simplify(
                                getDynamicFilterElysiumCompactionMultiplier(
                                        session));
                        dynamicPredicate = simplifyFilters(dynamicPredicate,
                                tableTS,
                                getDynamicFilterPushdownThreshold(session));
                        TupleDomain<VastColumnHandle> splitFilters = dynamicPredicate.intersect(
                                tableHandle
                                        .getPredicate()
                                        .intersect(partitionFilters));

                        if (!splitFilters.isNone()) {
                            long estimatedRowCount = (long) TypeUtils.getRowValue(
                                    TRINO_ROW_ESTIMATE_TRINO_TYPE,
                                    page.getBlock(estimateColIndex), row);
                            LOG.debug("Partition %s estimated row count: %d",
                                    domains, estimatedRowCount);
                            List<String> sortingColumnNames = tableHandle
                                    .getSortedColumns()
                                    .orElse(List.of());

                            boolean hasSortingColumnPredicate = tableHandle
                                    .getPredicate()
                                    .getDomains()
                                    .orElse(Map.of())
                                    .keySet()
                                    .stream()
                                    .anyMatch(
                                            col -> sortingColumnNames.contains(
                                                    col.getField().getName()));
                            if (hasSortingColumnPredicate && estimatedRowCount > getMinRowsForPartitionSplitEstimation(
                                    session)) {
                                querySplitSourceMetrics.incEstimateSize();
                                pendingPartitionPreEstimation.add(
                                        new PartitionSplitsLoader(
                                                vastPageSourceProvider, this,
                                                workerAddresses,
                                                estimatedRowCount, tableHandle,
                                                tableTS, splitFilters, tx,
                                                traceToken, schedulingInfo,
                                                session));
                            }
                            else {
                                int numOfSubSplits = getNumOfSubSplits(session);
                                if (splitFilters.isAll()) {
                                    numOfSubSplits = 1; // in full scan we don't need sub-splits
                                }

                                final int[] numOfSplits = estimateNumOfSplits(
                                        OptionalLong.of(estimatedRowCount),
                                        session, splitFilters, getNumOfSplits(session), numOfSubSplits, tableTS,
                                        tableHandle.getLimit());
                                LOG.debug("num of splits: %d, subSplits: %d",
                                        numOfSplits[SPLITS_IDX],
                                        numOfSplits[SUBSPLITS_IDX]);
                                List<ConnectorSplit> splits = createSplits(
                                        numOfSplits, rowGroupsPerSubSplit,
                                        workerAddresses, schedulingInfo,
                                        splitFilters);
                                ret.addAll(splits);
                                querySplitSourceMetrics.addInitialSplits(
                                        splits.size());
                            }
                        }
                    }
                }
            }
        }
        return ret;
    }

    private List<PartitionMetadata> getPartitionTableColumns(List<Field> tableFields,
                                                             List<Field> pitFields)
    {
        List<PartitionMetadata> partitionColumnMDs = new ArrayList<>();
        List<PartitionColumnMetadata> partitionColumns = tableHandle
                .getPartitionColumns()
                .orElseThrow();
        List<Field> pitValueFields = pitFields
                .stream()
                .filter(f -> !PIT_METADATA_COLUMN_NAMES.contains(f.getName()))
                .toList();

        for (PartitionColumnMetadata pcm : partitionColumns) {
            Field preTransformField = tableFields
                    .stream()
                    .filter(f -> f.getName().equals(pcm.sourceColumnName))
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException(
                            "Pre transform column " + pcm.sourceColumnName + " not found in table " + tableHandle.getTableName()));
            Field postTransformField = pitValueFields
                    .stream()
                    .filter(f -> f.getName().equals(pcm.columnName))
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException(
                            "PIT column " + pcm.columnName + " not found in pit of " + tableHandle.getTableName()));
            partitionColumnMDs.add(new PartitionMetadata(
                    VastColumnHandle.fromField(postTransformField),
                    VastColumnHandle.fromField(preTransformField)));
        }
        return partitionColumnMDs;
    }

    private List<VastSubstringMatch> convertQuerySubstringMatchers(List<PartitionMetadata> projectedPartitionValueColumns)
    {
        if (tableHandle.getSubstringMatches().isEmpty()) {
            return List.of();
        }
        List<VastSubstringMatch> partitionSubstringMatchers = new ArrayList<>();
        for (VastSubstringMatch substringMatch : tableHandle.getSubstringMatches()) {
            PartitionMetadata matchingPartition = findCorrespondingPartitionMetadata(
                    substringMatch.column(), projectedPartitionValueColumns);
            if (matchingPartition != null) {
                partitionSubstringMatchers.add(new VastSubstringMatch(
                        matchingPartition.tableColumnHandle,
                        substringMatch.pattern()));
            }
        }
        return partitionSubstringMatchers;
    }

    private TupleDomain<VastColumnHandle> convertQueryPredicates(List<Field> tableFields,
                                                                 List<PartitionMetadata> projectedPartitionValueColumns)
    {
        Map<VastColumnHandle, Domain> predicateDomains = new HashMap<>();
        for (Field field : tableFields) {
            VastColumnHandle vastColumnHandle = VastColumnHandle.fromField(
                    field);
            PartitionMetadata matchingPartition = findCorrespondingPartitionMetadata(
                    vastColumnHandle, projectedPartitionValueColumns);
            if (matchingPartition == null) {
                continue;
            }
            VastColumnHandle partitionColumnHandle = matchingPartition.tableColumnHandle;
            Domain finalDomain = Domain.all(
                    vastColumnHandle.getColumnMetadata().getType());
            Domain dynamicFilterDomain = dynamicFilter
                    .getCurrentPredicate()
                    .getDomain(vastColumnHandle,
                            vastColumnHandle.getColumnMetadata().getType());

            Domain partitionDomain = tableHandle
                    .getPredicate()
                    .getDomain(vastColumnHandle,
                            vastColumnHandle.getColumnMetadata().getType());
            if (partitionDomain != null) {
                finalDomain = finalDomain.intersect(partitionDomain);
            }
            if (dynamicFilterDomain != null && !dynamicFilterDomain.isNone()) {
                finalDomain = finalDomain.intersect(dynamicFilterDomain);
            }
            finalDomain = finalDomain.simplify(
                    getDynamicFilterElysiumCompactionMultiplier(session));
            predicateDomains.put(partitionColumnHandle, finalDomain);
        }
        return TupleDomain.withColumnDomains(predicateDomains);
    }

    private PartitionMetadata findCorrespondingPartitionMetadata(
            VastColumnHandle predicateColumnHandle,
            List<PartitionMetadata> projectedPartitionValueColumns)
    {
        for (PartitionMetadata md : projectedPartitionValueColumns) {
            String preTransformColumnName = md.tableColumnHandle
                    .getField()
                    .getName();

            if (preTransformColumnName.equals(
                    predicateColumnHandle.getField().getName())) {
                return md;
            }
        }
        return null;
    }

    @Override
    public boolean isSplitSourceFinished()
    {
        synchronized (pendingPartitionPreEstimation) {
            LOG.debug(
                    "isFinished: splitsQueueSize = %d, pendingPartitionPreEstimation=%s",
                    splitsQueue.size(), pendingPartitionPreEstimation);
            return splitsQueue.isEmpty() && pendingPartitionPreEstimation.isEmpty();
        }
    }

    void addPartitionLoaderSplits(List<ConnectorSplit> splits,
                                  PartitionSplitsLoader splitsLoader)
    {
        synchronized (pendingPartitionPreEstimation) {
            splitsQueue.addAll(splits);
            pendingPartitionPreEstimation.remove(splitsLoader);
        }
    }

    private record PartitionMetadata(VastColumnHandle partitionFilterColumnHandle,
            VastColumnHandle tableColumnHandle)
    {
        boolean isIdentity()
        {
            return this.partitionFilterColumnHandle
                    .getField()
                    .getName()
                    .equals(tableColumnHandle.getField().getName());
        }

        VastColumnHandle splitFilterColumnHandle()
        {
            return this.isIdentity() ?
                    tableColumnHandle :
                    partitionFilterColumnHandle;
        }
    }
}
