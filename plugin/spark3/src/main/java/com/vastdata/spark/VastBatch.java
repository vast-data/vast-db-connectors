/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastIOException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.spark.predicate.VastPredicate;
import com.vastdata.spark.statistics.SparkVastStatisticsManager;
import com.vastdata.spark.tx.VastAutocommitTransaction;
import ndb.NDB;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.util.NumOfSplitsEstimator.estimateNumberOfSplits;
import static com.vastdata.client.util.NumOfSplitsEstimator.longToDouble;
import static java.lang.String.format;

public class VastBatch
        implements Batch
{
    private static final SecureRandom batchIdProvider = new SecureRandom();
    private static final Logger LOG = LoggerFactory.getLogger(VastBatch.class);
    private final VastTable table;
    private StructType schema;
    private List<List<VastPredicate>> predicates;
    private final VastConfig vastConfig;
    private final Integer limit;
    private VastPartitionReaderFactory vastPartitionReaderFactory;
    private final int batchID = batchIdProvider.nextInt();
    private VastInputPartition[] splits;
    private static final AtomicBoolean describeFlag = new AtomicBoolean(true);
    private final boolean verbose = describeFlag.getAndSet(false);

    public VastBatch(VastTable table, StructType schema, Integer limit, List<List<VastPredicate>> predicates)
    {
        this.table = table;
        this.schema = schema;
        this.limit = limit;
        this.predicates = predicates;
        try {
            this.vastConfig = NDB.getConfig();
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
        LOG.info("new VastBatch: batchID={}, table={}, predicates={}", batchID, this.table.name(), this.predicates);
    }

    @Override
    public InputPartition[] planInputPartitions()
    {
        if (verbose) {
            LOG.warn(format("planInputPartitions()for batchID=%s, table=%s", batchID, table.name()), new Exception("VERBOSE BATCH"));
        }
        if (splits == null) {
            LOG.info("planInputPartitions() initializing for batchID={}, table={}, predicates={}", batchID, table.name(), predicates);
            IntSupplier numOfSplitsConfSupplier = vastConfig::getNumOfSplits;
            LongSupplier rowPerSplitSupplier = vastConfig::getQueryDataRowsPerSplit;
            Supplier<Optional<Double>> rowsEstimateSupplier = () -> SparkVastStatisticsManager.getInstance().getTableStatistics(table).map(s -> s.rowCount().isEmpty() ? null : longToDouble(s.rowCount().get().longValue()));
            int numOfSplits = estimateNumberOfSplits(numOfSplitsConfSupplier, rowPerSplitSupplier, rowsEstimateSupplier);
            int numOfSplitsConf = numOfSplitsConfSupplier.getAsInt();
            if (numOfSplits < numOfSplitsConf) {
                LOG.info("Reduced splits number for batchID={}, table={} from {} to {}", batchID, table.name(), numOfSplitsConf, numOfSplits);
            }
            splits = IntStream
                    .range(0, numOfSplits)
                    .mapToObj(i -> new VastInputPartition(i, batchID, numOfSplits))
                    .toArray(VastInputPartition[]::new);
        }
        else {
            LOG.info("planInputPartitions() returning ready splits for batchID={}, table={}, predicates={}", batchID, table.name(), predicates);
        }
        return splits;
    }

    @Override
    public PartitionReaderFactory createReaderFactory()
    {
        if (vastPartitionReaderFactory == null) {
            LOG.info("{}:{} createReaderFactory() NEW predicates={}", batchID, table.name(), predicates);
            vastPartitionReaderFactory = new VastPartitionReaderFactory(getTx(), batchID, vastConfig, table.getTableMD().schemaName, table.getTableMD().tableName, schema, limit, predicates);
        }
        else {
            LOG.info("{}:{} createReaderFactory() RETURN predicates={}", batchID, table.name(), predicates);
        }
        return vastPartitionReaderFactory;
    }

    private SimpleVastTransaction getTx()
    {
        try {
            return VastAutocommitTransaction.getExisting();
        }
        catch (VastIOException e) {
            throw new IllegalStateException("Failed getting existing transaction", e);
        }
    }

    public void updateProjections(StructType schema)
    {
        LOG.info("updateProjections VastBatch: batchID={}, table={}, projections={}", batchID, table.name(), schema);
        this.schema = schema;
        if (vastPartitionReaderFactory != null) {
            vastPartitionReaderFactory.updateProjections(schema);
        }
    }

    void updatePushdownPredicates(List<List<VastPredicate>> pushDownPredicates)
    {
        LOG.info("updatePushdownPredicates VastBatch: batchID={}, table={}, predicates={}", batchID, table.name(), pushDownPredicates);
        this.predicates = pushDownPredicates;
        if (vastPartitionReaderFactory != null) {
            vastPartitionReaderFactory.updatePushdownPredicates(this.predicates);
        }
    }

    public VastTable getTable()
    {
        return table;
    }
}
