/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.FlatBufferSerializer;
import com.vastdata.client.QueryDataExtraParams;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.VastSplitContext;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.EnumeratedSchema;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastAutocommitTransaction;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransactionFactory;
import com.vastdata.spark.adaptor.ArrowToSparkResultAdaptor;
import com.vastdata.spark.predicate.VastPredicate;
import com.vastdata.spark.statistics.SparkVastStatisticsManager;
import com.vastdata.spark.tx.VastSparkTransactionsManager;
import ndb.NDB;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.MutableProjection;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.apache.spark.sql.catalyst.util.SparkCharVarcharUtils$;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import spark.sql.catalog.ndb.TypeUtil;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.schema.VastMetadataUtils.SORTED_BY_PROPERTY;
import static com.vastdata.client.util.NumOfSplitsEstimator.SPLITS_IDX;
import static com.vastdata.client.util.NumOfSplitsEstimator.SUBSPLITS_IDX;
import static com.vastdata.client.util.NumOfSplitsEstimator.getNumOfSplitsEstimation;
import static com.vastdata.client.util.SchedulingInfoProvider.getVastSchedulingInfo;
import static com.vastdata.spark.AlwaysFalseFilterUtil.isAlwaysFalsePredicate;
import static com.vastdata.spark.predicate.VastPredicates.Equal;
import static com.vastdata.spark.predicate.VastPredicates.IsNull;
import static com.vastdata.spark.statistics.FilterEstimator.estimateSelectivity;
import static com.vastdata.spark.statistics.StatsUtils.sparkCatalystStatsToTableStatistics;
import static java.lang.String.format;
import static java.util.Objects.hash;
import static ndb.NDBScanTransactionSupplier.supplyTransaction;
import static ndb.NDBSparkSessionExtension.getSessionUser;

public class VastBatch
        implements Batch
{
    public static final InputPartition SINGLE_SPLIT_INPUT_PARTITION = new VastInputPartition(
            0, 0, 1, 1);
    private static final SecureRandom batchIdProvider = new SecureRandom();
    private static final Logger LOG = LoggerFactory.getLogger(VastBatch.class);
    protected final VastTable table;
    protected final StructType schema;
    protected final VastConfig vastConfig;
    protected final Integer limit;
    protected final int batchID = batchIdProvider.nextInt();
    private final VastClient vastClient;
    protected List<List<VastPredicate>> predicates;
    private VastPartitionReaderFactory vastPartitionReaderFactory;

    public VastBatch(VastTable table, StructType schema, Integer limit,
            List<List<VastPredicate>> predicates)
    {
        this.table = table;
        this.schema = schema;
        this.limit = limit;
        this.predicates = predicates;
        try {
            this.vastConfig = NDB.getConfig();
            this.vastClient = NDB.getVastClient(this.vastConfig);
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
        LOG.info(
                "{}:{} new VastBatch: schema={}, predicates={}",
                batchID, this.table.name(), this.schema, this.predicates);
    }

    private List<VastKeyGroupedInputPartition> prunePartitions(
            Optional<Statistics> statistics)
    {
        VastPartitionedTable ptable = (VastPartitionedTable) table;
        List<NamedReference> refs = ptable.refForPartitioning();
        Map<Boolean, List<List<VastPredicate>>> splitPredicates = predicates
                .stream()
                .collect(Collectors.partitioningBy(
                        l -> refs.contains(l.get(0).getReference())));
        List<List<VastPredicate>> partitionPredicates = splitPredicates
                .get(true);
        LOG.debug("{}:{} PartitionPredicates: {}", batchID, table, partitionPredicates);
        List<StructField> refFields = Arrays.asList(
                ptable.partitionSchema().fields());
        ArrayList<VastKeyGroupedInputPartition> rv = new ArrayList<>();
        List<List<VastPredicate>> filterPredicates = splitPredicates.get(
                false);

        List<org.apache.spark.sql.catalyst.expressions.Expression> projRefs = new ArrayList<>(
                refs.size());
        for (int i = 0; i < refs.size(); i++) {
            StructField of = refFields.get(i);
            projRefs.add(new BoundReference(i, of.dataType(), of.nullable()));
        }
        Seq<org.apache.spark.sql.catalyst.expressions.Expression> projRefsSeq = JavaConverters
                .asScalaBuffer(projRefs)
                .toSeq();
        MutableProjection keysProj = MutableProjection.create(projRefsSeq);

        List<NamedReference> partitionRefs = ptable.partitionRefs();
        List<List<VastPredicate>> elysiumPredicates;
        List<List<VastPredicate>> elysiumFilters;
        String sortedColumnsS = table.properties().get(SORTED_BY_PROPERTY);

        if (sortedColumnsS != null) { // lyssandra table
            List<String> sortedColumns = Arrays.asList(sortedColumnsS.split(","));
            Map<Boolean, List<List<VastPredicate>>> elysiumSplitPredicates = filterPredicates.stream()
                    .collect(Collectors.partitioningBy(l -> sortedColumns.contains(l.get(0)
                            .getReference()
                            .toString())));
            if (!elysiumSplitPredicates.get(true).isEmpty()) {
                elysiumPredicates = elysiumSplitPredicates.get(true);
                elysiumFilters = elysiumSplitPredicates.get(false);
            }
            else {
                elysiumFilters = null;
                elysiumPredicates = null;
            }
        }
        else {
            elysiumFilters = null;
            elysiumPredicates = null;
        }

        Consumer<InternalRow> rowConsumer = row -> {
            long estimate = row.getLong(refs.size());
            LOG.debug("{}:{} hydra rows estimate: {}", batchID, table.name(), estimate);
            InternalRow key = keysProj.apply(row);
            List<List<VastPredicate>> fp = filterPredicates;
            List<List<VastPredicate>> generatedPredicates = new ArrayList<>(key.numFields());

            for (int i = 0; i < key.numFields(); i++) {
                if (key.isNullAt(i)) {
                    generatedPredicates.add(Arrays.asList(new VastPredicate(new IsNull(partitionRefs.get(i)), partitionRefs.get(i), refFields.get(i))));
                }
                else {
                    DataType dt = SparkCharVarcharUtils$.MODULE$.replaceCharVarcharWithString(refFields.get(i).dataType());
                    generatedPredicates.add(Arrays.asList(new VastPredicate(new Equal(partitionRefs.get(i), dt, key.get(i, dt)), partitionRefs.get(i), refFields.get(i))));
                }
            }

            // If the partition is sufficiently big, then we ask for a Lysandra estimate and take the smaller one
            if (elysiumPredicates != null && estimate > 2 * vastConfig.getQueryDataRowsPerSplit()) {
                ArrayList<List<VastPredicate>> countPredicates = new ArrayList<>(elysiumPredicates);
                countPredicates.addAll(generatedPredicates);

                try {
                    final long elysiumEstimate = getElysiumRowCount(countPredicates);
                    LOG.info("elysium row count: {}, hydra estimate {}", elysiumEstimate, estimate);

                    if (elysiumEstimate < estimate) {
                        estimate = elysiumEstimate;
                        fp = elysiumFilters;
                    }
                }
                catch (Throwable t) {
                    LOG.warn("Lysandra: Caught exception when trying to get elysium rowcount", t);
                }
            }

            final int[] numOfSplits = getNumOfSplits(OptionalLong.of(estimate), fp, statistics);
            LOG.debug("{}:{} num of splits: {}, subSplits: {}",
                    batchID, table.name(), numOfSplits[SPLITS_IDX], numOfSplits[SUBSPLITS_IDX]);
            IntStream
                    .range(0, numOfSplits[SPLITS_IDX])
                    .mapToObj(i -> new VastKeyGroupedInputPartition(
                            generatedPredicates,
                            i,
                            batchID,
                            numOfSplits[SPLITS_IDX],
                            numOfSplits[SUBSPLITS_IDX],
                            key))
                    .forEachOrdered(rv::add);
        };

        VastPITTable adaptedTableToPITScan = ptable.forPITScan();
        VastPITBatch vastPITBatch = new VastPITBatch(adaptedTableToPITScan,
                adaptedTableToPITScan.readSchema(), partitionPredicates);
        try (PartitionReader<ColumnarBatch> pitReader = vastPITBatch
                .createReaderFactory()
                .createColumnarReader(SINGLE_SPLIT_INPUT_PARTITION)) {
            while (pitReader.next()) {
                ColumnarBatch columnarBatch = pitReader.get();
                Iterator<InternalRow> rowIt = columnarBatch.rowIterator();
                while (rowIt.hasNext()) {
                    InternalRow row = rowIt.next();
                    rowConsumer.accept(row);
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed closing PIT reader", e);
        }

        // TODO: we might want to simplify the predicates here
        // predicates = filterPredicates;
        return rv;
    }

    private long getElysiumRowCount(List<List<VastPredicate>> preds)
            throws VastUserException
    {
        final String endUser = getSessionUser(vastConfig);
        final int rowGroupsPerSubSplit = vastConfig.getRowGroupsPerSubSplit();
        final VastSplitContext getSizeContext = new VastSplitContext(
                0xffffffffL - 3, 1, 1, rowGroupsPerSubSplit);
        final String tableName = table.getTableMD().tableName;
        VastClient client = NDB.getVastClient(vastConfig);
        VastSparkTransactionsManager transactionsManager = VastSparkTransactionsManager.getInstance(
                client, new VastTransactionFactory());
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(
                transactionsManager,
                () -> transactionsManager.startTransaction(endUser), endUser)) {
            SimpleVastTransaction transaction = new SimpleVastTransaction(
                    tx.getId());

            LOG.debug("{}:{} getElysiumRowCount tx={}",
                    batchID, tableName, transaction);
            VastTraceToken token = transaction.generateTraceToken(
                    Optional.of(format("getRowCount %s", tableName)));
            LinkedHashSet<Field> fields = new LinkedHashSet<>();
            preds.forEach(list -> list.forEach(vp -> fields.add(
                    TypeUtil.sparkFieldToArrowField(vp.getField()))));
            EnumeratedSchema enumeratedSchema = new EnumeratedSchema(fields);
            Schema projectionSchema = new Schema(Collections.emptyList());
            FlatBufferSerializer projectionSerializer = new SparkProjectionSerializer(
                    projectionSchema, enumeratedSchema);
            FlatBufferSerializer predicateSerializer = new SparkPredicateSerializer(
                    token.toString(), preds, enumeratedSchema);
            ArrowToSparkResultAdaptor<ColumnarBatch> adaptor = new ArrowToSparkResultAdaptor<>(
                    ColumnarBatch.class, ColumnVector.class,
                    ArrowColumnVector::new);
            final CommonVastColumnarBatchReader<ColumnarBatch> vastReader = new CommonVastColumnarBatchReader<>(
                    client, null, getSizeContext, vastConfig,
                    projectionSerializer, predicateSerializer, transaction,
                    token, table.getSchemaName(), tableName, enumeratedSchema,
                    projectionSchema, false, new RootAllocator(), adaptor,
                    ColumnarBatch::numRows, transactionsManager, false, null,
                    new QueryDataExtraParams(), endUser);
            vastReader.next();
            ColumnarBatch batch = vastReader.get();
            return ((long) batch.numRows()) << 16;
        }
    }

    private int[] getNumOfSplits(OptionalLong rowCount,
            List<List<VastPredicate>> preds, Optional<Statistics> stats)
    {
        String traceToken = "table_name=" + table.name() + ", " + "batch_id=" + batchID;
        return getNumOfSplitsEstimation(rowCount, vastConfig.getNumOfSplits(),
                vastConfig.getNumOfSubSplits(),
                vastConfig.getRowGroupsPerSubSplit(),
                vastConfig.getQueryDataRowsPerPage(), stats.isPresent() ?
                        estimateSelectivity(preds,
                                sparkCatalystStatsToTableStatistics(
                                        stats.get()), traceToken) :
                        1.0, vastConfig.getQueryDataRowsPerSplit(),
                vastConfig.getAdaptivePartitioning());
    }

    @Override
    public InputPartition[] planInputPartitions()
    {
        LOG.info(
                "planInputPartitions() initializing for batchID={}, table={}, predicates={}",
                batchID, table.name(), predicates);

        if (isAlwaysFalsePredicate(predicates)) {
            return new InputPartition[0];
        }

        Optional<Statistics> statistics = SparkVastStatisticsManager
                .getInstance()
                .getTableStatistics(table);

        if (table instanceof VastPartitionedTable) {
            LOG.debug("{}:{} Partitioned read", batchID, table.name());
            return prunePartitions(statistics).toArray(new VastKeyGroupedInputPartition[0]);
        }

        OptionalLong rowCount = (statistics.isPresent() && !statistics
                .get()
                .rowCount()
                .isEmpty()) ?
                OptionalLong.of(statistics.get().rowCount().get().longValue()) :
                OptionalLong.empty();
        List<List<VastPredicate>> relevantPredicates = predicates;
        String sortedColumnsS = table.properties().get(SORTED_BY_PROPERTY);
        if (sortedColumnsS != null) { // elysium table
            // TODO hydra-elysium combo comes later
            List<String> sortedColumns = Arrays.asList(
                    sortedColumnsS.split(","));
            Map<Boolean, List<List<VastPredicate>>> splitPredicates = predicates
                    .stream()
                    .collect(Collectors.partitioningBy(
                            l -> sortedColumns.contains(l
                                    .get(0)
                                    .getReference()
                                    .toString())));
            relevantPredicates = splitPredicates.getOrDefault(false,
                    Collections.emptyList());
            if (!rowCount.isPresent() || splitPredicates.containsKey(true)) {
                try {
                    long elysiumRowCount = getElysiumRowCount(
                            splitPredicates.getOrDefault(true,
                                    Collections.emptyList()));
                    LOG.debug(
                            "{}:{} Row count from elysium: {}, Row count from stats{}",
                            batchID, table.name(), elysiumRowCount, rowCount);
                    if (elysiumRowCount < rowCount.getAsLong()) {
                        rowCount = OptionalLong.of(elysiumRowCount);
                    }
                }
                catch (Throwable t) {
                    LOG.warn(
                            format("%s:%s Caught exception when trying to get elysium rowcount", batchID, table.name()),
                            t);
                }
            }
        }
        final int numOfSplitsConf = vastConfig.getNumOfSplits();
        final int[] numOfSplits = getNumOfSplits(rowCount, relevantPredicates,
                statistics);

        LOG.info("{}:{} Using num of splits: {} {}",
                batchID, table.name(), numOfSplits, reducedSplitsLogSuffix(numOfSplits, numOfSplitsConf));

        return IntStream.range(0, numOfSplits[SPLITS_IDX]).mapToObj(
                i -> new VastInputPartition(i, batchID, numOfSplits[SPLITS_IDX],
                        numOfSplits[SUBSPLITS_IDX])).toArray(
                VastInputPartition[]::new);
    }

    private static String reducedSplitsLogSuffix(int[] numOfSplits, int numOfSplitsConf)
    {
        return numOfSplits[SPLITS_IDX] < numOfSplitsConf ? format(". Reduced from %s", numOfSplitsConf) : "";
    }

    @Override
    public PartitionReaderFactory createReaderFactory()
    {
        if (vastPartitionReaderFactory == null) {
            LOG.info("{}:{} createReaderFactory() NEW predicates={}, schema={}",
                    batchID, table.name(), predicates, schema);
            SimpleVastTransaction tx = supplyTransaction();
            VastSchedulingInfo vastSchedulingInfo = getVastSchedulingInfo(
                    table.getTableMD().schemaName, table.getTableMD().tableName,
                    tx, this.vastClient, null);
            vastPartitionReaderFactory = new VastPartitionReaderFactory(tx,
                    batchID, vastConfig, table.getTableMD().schemaName,
                    table.getTableMD().tableName, schema, limit, predicates,
                    vastSchedulingInfo);
            if (table.getTableMD().isForUpdate() || table
                    .getTableMD()
                    .isForDelete()) {
                vastPartitionReaderFactory.setForAlter();
            }
        }
        else {
            LOG.info(
                    "{}:{} createReaderFactory() RETURN predicates={}, schema={}",
                    batchID, table.name(), predicates, schema);
        }
        return vastPartitionReaderFactory;
    }

    void updatePushdownPredicates(List<List<VastPredicate>> pushDownPredicates)
    {
        LOG.info(
                "{}:{} updatePushdownPredicates predicates={}",
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

    private List<List<String>> formatPredicates()
    {
        return this.predicates.stream().map(vpList -> vpList
                .stream()
                .map(VastPredicate::toString)
                .collect(Collectors.toList())).collect(Collectors.toList());
    }

    public String description()
    {
        return toStringHelper(this).add("table_name", this.table.name()).add(
                "schema", this.schema.toString()).add("pushed_down_limit",
                this.limit).add("pushed_down_predicates",
                this.formatPredicates()).add("partition_reader_factory",
                this.vastPartitionReaderFactory).toString();
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
