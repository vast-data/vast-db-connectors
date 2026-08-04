/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastAutocommitTransaction;
import com.vastdata.spark.predicate.VastPredicate;
import com.vastdata.spark.statistics.SparkVastStatisticsManager;
import ndb.NDB;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import java.util.stream.IntStream;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.util.NumOfSplitsEstimator.SPLITS_IDX;
import static com.vastdata.client.util.NumOfSplitsEstimator.SUBSPLITS_IDX;
import static com.vastdata.client.util.NumOfSplitsEstimator.getNumOfSplitsEstimation;
import static com.vastdata.client.util.SchedulingInfoProvider.getVastSchedulingInfo;
import static com.vastdata.spark.statistics.FilterEstimator.estimateSelectivity;
import static com.vastdata.spark.statistics.StatsUtils.sparkCatalystStatsToTableStatistics;
import static java.lang.String.format;
import static java.util.Objects.hash;

public class VastBatch
        implements Batch
{
    private static final SecureRandom batchIdProvider = new SecureRandom();
    private static final Logger LOG = LoggerFactory.getLogger(VastBatch.class);
    private static final AtomicBoolean describeFlag = new AtomicBoolean(false);
    private final VastTable table;
    private final StructType schema;
    private final Map<String, String> columnMasks;
    private final VastConfig vastConfig;
    private final Integer limit;
    private final int batchID = batchIdProvider.nextInt();
    private final boolean verbose = describeFlag.getAndSet(false);
    private final VastClient vastClient;
    private List<List<VastPredicate>> predicates;
    private VastPartitionReaderFactory vastPartitionReaderFactory;

    public VastBatch(VastTable table, StructType schema, Integer limit,
            List<List<VastPredicate>> predicates,
            Map<String, String> columnMasks)
    {
        this.table = table;
        this.schema = schema;
        this.limit = limit;
        this.predicates = predicates;
        this.columnMasks = columnMasks;
        try {
            this.vastConfig = NDB.getConfig();
            this.vastClient = NDB.getVastClient(this.vastConfig);
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
        LOG.info("new VastBatch: batchID={}, table={}, predicates={}", batchID,
                this.table.name(), this.predicates);
    }

    @Override
    public InputPartition[] planInputPartitions()
    {
        if (verbose) {
            LOG.warn(format("planInputPartitions()for batchID=%s, table=%s",
                    batchID, table.name()), new Exception("VERBOSE BATCH"));
        }
        LOG.info(
                "planInputPartitions() initializing for batchID={}, table={}, predicates={}",
                batchID, table.name(), predicates);
        final IntSupplier numOfSplitsConfSupplier = vastConfig::getNumOfSplits;
        Optional<Statistics> statistics = SparkVastStatisticsManager
                .getInstance()
                .getTableStatistics(table);
        final OptionalLong rowCount = (statistics.isPresent() && !statistics
                .get()
                .rowCount()
                .isEmpty()) ?
                OptionalLong.of(statistics.get().rowCount().get().longValue()) :
                OptionalLong.empty();
        final int numOfSplitsConf = numOfSplitsConfSupplier.getAsInt();
        String traceToken = "table_name=" + table.name() + ", " + "batch_id=" + batchID;
        final int[] numOfSplits = getNumOfSplitsEstimation(rowCount,
                numOfSplitsConf, vastConfig.getNumOfSubSplits(),
                vastConfig.getRowGroupsPerSubSplit(),
                vastConfig.getQueryDataRowsPerPage(), statistics.isPresent() ?
                        estimateSelectivity(predicates,
                                sparkCatalystStatsToTableStatistics(
                                        statistics.get()), traceToken) :
                        1.0, vastConfig.getQueryDataRowsPerSplit(),
                vastConfig.getAdaptivePartitioning());

        if (numOfSplits[SPLITS_IDX] < numOfSplitsConf) {
            LOG.info(
                    "Reduced splits number for batchID={}, table={} from {} to {}",
                    batchID, table.name(), numOfSplitsConf, numOfSplits);
        }
        return IntStream.range(0, numOfSplits[SPLITS_IDX]).mapToObj(
                i -> new VastInputPartition(i, batchID, numOfSplits[SPLITS_IDX],
                        numOfSplits[SUBSPLITS_IDX])).toArray(
                VastInputPartition[]::new);
    }

    @Override
    public PartitionReaderFactory createReaderFactory()
    {
        return createReaderFactory(null);
    }

    public PartitionReaderFactory createReaderFactory(SimpleVastTransaction tx)
    {
        if (vastPartitionReaderFactory == null) {
            LOG.info("{}:{} createReaderFactory() NEW predicates={}", batchID,
                    table.name(), predicates);
            SimpleVastTransaction realTx = getOrCreateTx(tx);
            VastSchedulingInfo vastSchedulingInfo = getVastSchedulingInfo(
                    table.getTableMD().schemaName, table.getTableMD().tableName,
                    realTx, this.vastClient, null);
            vastPartitionReaderFactory = new VastPartitionReaderFactory(realTx,
                    batchID, vastConfig, table.getTableMD().schemaName,
                    table.getTableMD().tableName, schema, limit, predicates,
                    vastSchedulingInfo);
        }
        else {
            LOG.info("{}:{} createReaderFactory() RETURN predicates={}",
                    batchID, table.name(), predicates);
        }
        return vastPartitionReaderFactory;
    }

    private SimpleVastTransaction getOrCreateTx(SimpleVastTransaction tx)
    {
        if (tx != null) {
            return tx;
        }
        return VastAutocommitTransaction.getExisting();
    }

    void updatePushdownPredicates(List<List<VastPredicate>> pushDownPredicates)
    {
        LOG.info(
                "updatePushdownPredicates VastBatch: batchID={}, table={}, predicates={}",
                batchID, table.name(), pushDownPredicates);
        this.predicates = pushDownPredicates;
        if (vastPartitionReaderFactory != null) {
            vastPartitionReaderFactory.updatePushdownPredicates(
                    this.predicates);
        }
    }

    public VastTable getTable()
    {
        return table;
    }

    @Override
    public int hashCode()
    {
        return hash(table.name(), schema, predicates, limit);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof VastBatch)) {
            return false;
        }
        VastBatch other = (VastBatch) o;
        return table.name().equals(other.table.name()) && schema.equals(
                other.schema) && predicates.equals(
                other.predicates) && ((limit == null && other.limit == null) || (limit != null && other.limit != null && limit.equals(
                other.limit)));
    }
}
