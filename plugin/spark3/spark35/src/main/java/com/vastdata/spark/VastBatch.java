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
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.util.NumOfSplitsEstimator.getNumOfSplitsEstimation;
import static com.vastdata.spark.statistics.FilterEstimator.estimateSelectivity;
import static com.vastdata.spark.statistics.FilterEstimator.getSizePerRow;
import static com.vastdata.spark.statistics.StatsUtils.sparkCatalystStatsToTableStatistics;
import static java.lang.String.format;
import static java.util.Objects.hash;

public class VastBatch
        implements Batch
{
    private static final SecureRandom batchIdProvider = new SecureRandom();
    private static final Logger LOG = LoggerFactory.getLogger(VastBatch.class);
    private final VastTable table;
    private final StructType schema;
    private List<List<VastPredicate>> predicates;
    private final String tablePath;
    private final VastConfig vastConfig;
    private final Integer limit;
    private VastPartitionReaderFactory vastPartitionReaderFactory;
    private final int batchID = batchIdProvider.nextInt();
    private static final AtomicBoolean describeFlag = new AtomicBoolean(false);
    private final boolean verbose = describeFlag.getAndSet(false);

    public VastBatch(VastTable table, StructType schema, Integer limit, List<List<VastPredicate>> predicates)
    {
        this.table = table;
        this.schema = schema;
        this.limit = limit;
        this.predicates = predicates;
        this.tablePath = "/" + table.name(); // table name doesn't start with "/".
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
        LOG.info("planInputPartitions() initializing for batchID={}, table={}, predicates={}", batchID, table.name(), predicates);
        final IntSupplier numOfSplitsConfSupplier = vastConfig::getNumOfSplits;
        final LongSupplier rowsPerSplitConf = vastConfig::getQueryDataRowsPerSplit;
        Optional<Statistics> statistics = SparkVastStatisticsManager.getInstance().getTableStatistics(table);
	final OptionalLong rowCount = (statistics.isPresent() && !statistics.get().rowCount().isEmpty())?
	    OptionalLong.of(statistics.get().rowCount().get().longValue()) : OptionalLong.empty();
        Supplier<Optional<Double>> rowsEstimateSupplier =
	    () -> rowCount.isPresent()? Optional.of((double)rowCount.getAsLong()) : Optional.empty();
        BooleanSupplier useMultiplier = () -> rowCount.isPresent() && vastConfig.getAdaptivePartitioning();
        DoubleSupplier selectivityEstimation = () -> estimateSelectivity(predicates, sparkCatalystStatsToTableStatistics(statistics.get()));
        LongSupplier multiplierConf = vastConfig::getSplitSizeMultiplier;
        final int numOfSplits =
            getNumOfSplitsEstimation(useMultiplier, selectivityEstimation, numOfSplitsConfSupplier, rowsPerSplitConf, multiplierConf, rowsEstimateSupplier);
        final int numOfSplitsConf = numOfSplitsConfSupplier.getAsInt();
        if (numOfSplits < numOfSplitsConf) {
            LOG.info("Reduced splits number for batchID={}, table={} from {} to {}", batchID, table.name(), numOfSplitsConf, numOfSplits);
        }
        return IntStream
                .range(0, numOfSplits)
                .mapToObj(i -> new VastInputPartition(tablePath, i, batchID, numOfSplits))
                .toArray(VastInputPartition[]::new);
    }

    @Override
    public PartitionReaderFactory createReaderFactory()
    {
        if (vastPartitionReaderFactory == null) {
            LOG.info("{}:{} createReaderFactory() NEW predicates={}, schema={}", batchID, table.name(), predicates, schema);
            vastPartitionReaderFactory = new VastPartitionReaderFactory(getTx(), batchID, vastConfig, table.getTableMD().schemaName, table.getTableMD().tableName, schema, limit, predicates);
            if (table.getTableMD().isForUpdate() || table.getTableMD().isForDelete()) {
                vastPartitionReaderFactory.setForAlter();
            }
        }
        else {
            LOG.info("{}:{} createReaderFactory() RETURN predicates={}, schema={}", batchID, table.name(), predicates, schema);
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


    private List<List<String>> formatPredicates()
    {
        return this.predicates
                .stream()
                .map(vpList ->
                        vpList
                                .stream()
                                .map(VastPredicate::toString)
                                .collect(Collectors.toList())
                )
                .collect(Collectors.toList());
    }

    public String description() {
        return toStringHelper(this)
                .add("table_name", this.table.name())
                .add("schema", this.schema.toString())
                .add("pushed_down_limit", this.limit)
                .add("pushed_down_predicates", this.formatPredicates())
                .add("partition_reader_factory", this.vastPartitionReaderFactory)
                .toString();
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
        VastBatch other = (VastBatch)o;
        return table.name().equals(other.table.name()) && schema.equals(other.schema) && predicates.equals(other.predicates) &&
                ((limit == null && other.limit == null) || (limit != null && other.limit != null && limit.equals(other.limit)));
    }
}
