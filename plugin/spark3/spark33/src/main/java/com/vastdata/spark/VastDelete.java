/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.collect.ImmutableList;
import com.google.flatbuffers.FlatBufferBuilder;
import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.EnumeratedSchema;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.spark.predicate.VastPredicate;
import com.vastdata.spark.predicate.VastPredicatePushdown;
import com.vastdata.spark.tx.VastAutocommitTransaction;
import com.vastdata.spark.tx.VastSimpleTransactionFactory;
import com.vastdata.spark.tx.VastSparkTransactionsManager;
import ndb.NDB;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.connector.catalog.SupportsDelete;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.sources.AlwaysFalse;
import org.apache.spark.sql.sources.AlwaysTrue;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.sql.catalog.ndb.TypeUtil;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_UINT64_FIELD;
import static com.vastdata.client.schema.RowIDVectorCopy.copyVectorBuffers;
import static java.lang.String.format;

public class VastDelete
        implements SupportsDelete
{

    private static final Logger LOG = LoggerFactory.getLogger(VastDelete.class);

    protected static final String DELETE_UNSUPPORTED_ERROR = "Row level delete is not supported for required filters";
    public static final Function<Object, RuntimeException> DELETE_ERROR_SUPPLIER  = filters -> {
        String s;
        if (filters.getClass().isArray()) {
            s = Arrays.toString((Object[]) filters);
        }
        else {
            s = filters.toString();
        }
        return new IllegalArgumentException(format("%s: %s", DELETE_UNSUPPORTED_ERROR, s));
    };
    private static final BiPredicate<VastPredicatePushdown, EnumeratedSchema> isAllPushed = (res, schema) -> res.getPostFilter().isEmpty() && serializePushedPredicatesSuccessful(res.getPushedDown(), schema);
    private EnumeratedSchema enumeratedSchema;

    private static boolean serializePushedPredicatesSuccessful(List<List<VastPredicate>> pushedDown, EnumeratedSchema schema)
    {
        FlatBufferBuilder builder = new FlatBufferBuilder(128);
        try {
            int serialize = new SparkPredicateSerializer("VastDelete", pushedDown, schema).serialize(builder);
            LOG.debug("Serialization of pushed-down predicates passed: {} = {}", pushedDown, serialize);
            return serialize > 0;
        }
        catch (Throwable t) {
            LOG.warn(format("Serialization of pushed-down predicates fails: %s", pushedDown), t);
            builder.clear();
            return false;
        }
    }

    private static final Function<Filter[], Predicate[]> v2Adapt = filters -> Arrays.stream(filters).map(Filter::toV2).toArray(Predicate[]::new);
    private final Function<Predicate[], VastPredicatePushdown> parsePredicates;
    private final Supplier<VastClient> clientSupplier;
    private final VastTable table;

    public VastDelete(VastTable table, Supplier<VastClient> clientSupplier)
    {
        this.clientSupplier = clientSupplier;
        this.table = table;
        this.parsePredicates = predicates -> VastPredicatePushdown.parse(predicates, table.schema());
    }

    private static boolean isAlwaysTrue(Filter[] filters)
    {
        if (filters.length != 1)
            return false;
        return filters[0].equals(new AlwaysTrue());
    }

    private static boolean isAlwaysFalse(Filter[] filters)
    {
        if (filters.length != 1)
            return false;
        return filters[0].equals(new AlwaysFalse());
    }

    @Override
    public boolean canDeleteWhere(Filter[] filters)
    {
        if (isAlwaysTrue(filters) || isAlwaysFalse(filters)) {
            LOG.info("canDeleteWhere({}.{}) {} handling Always filters", this.table.getTableMD().schemaName, this.table.getTableMD().tableName, Arrays.toString(filters));
            return true;
        }
        boolean canDelete = isAllPushed.test(v2Adapt.andThen(this.parsePredicates).apply(filters), extractSchema());
        LOG.info("canDeleteWhere({}.{}) {} --> {}", this.table.getTableMD().schemaName, this.table.getTableMD().tableName, Arrays.toString(filters), canDelete);
        if (!canDelete) {
            throw DELETE_ERROR_SUPPLIER.apply(filters);
        }
        return true;
    }

    @NotNull
    private EnumeratedSchema extractSchema()
    {
        if (enumeratedSchema == null) {
            enumeratedSchema = new EnumeratedSchema(TypeUtil.sparkSchemaToArrowFieldsList(table.schema()));
        }
        return enumeratedSchema;
    }

    @Override
    public void deleteWhere(Filter[] filters)
    {
        LOG.info("deleteWhere({}.{}) {}", this.table.getTableMD().schemaName, this.table.getTableMD().tableName, Arrays.toString(filters));
        if (isAlwaysFalse(filters)) {
            // Nothing to do
            return;
        }
        if (isAlwaysTrue(filters)) {
            try {
                /* TODO: consider checking for table size and through exception if above certain value */
                delete(new Filter[0]);
            }
            catch (VastRuntimeException re) {
                throw re;
            }
            catch (Throwable t) {
                throw toRuntime(t);
            }
            return;
        }
        VastPredicatePushdown predicatePushdown = v2Adapt.andThen(this.parsePredicates).apply(filters);
        if (isAllPushed.test(predicatePushdown, extractSchema())) {
            try {
                delete(filters);
            }
            catch (VastRuntimeException re) {
                throw re;
            }
            catch (Throwable t) {
                throw toRuntime(t);
            }
        }
        else {
            throw DELETE_ERROR_SUPPLIER.apply(filters);
        }
    }

    private void delete(Filter[] filters)
            throws Throwable
    {
        VastClient client = clientSupplier.get();
        Supplier<URI> endpointsSupplier = getClientsSupplier();
        ForkJoinPool forkJoinPool = new ForkJoinPool(16);
        Set<VastColumnarBatchReader> readersForVerifyAllocation = new HashSet<>();
        LinkedBlockingQueue<FieldVector> pages = new LinkedBlockingQueue<>(1000);

        Optional<VastTraceToken> tokenHolder = Optional.empty();
        VastSparkTransactionsManager transactionsManager = VastSparkTransactionsManager.getInstance(client, new VastSimpleTransactionFactory());
        try (VastAutocommitTransaction tx = getTransaction(client, transactionsManager)) {
            VastTraceToken token = tx.generateTraceToken(Optional.empty());
            tokenHolder = Optional.of(token);
            VastBatch batch = (VastBatch) getBatch(filters);
            VastInputPartition[] inputPartitions = (VastInputPartition[]) batch.planInputPartitions();
            VastPartitionReaderFactory readerFactory = (VastPartitionReaderFactory) batch.createReaderFactory(new SimpleVastTransaction(tx.getId(), tx.isReadOnly(), false));
            readerFactory.setForAlter();
            AtomicBoolean error = new AtomicBoolean(false);
            Function<VastInputPartition, ForkJoinTask<Boolean>> queryDataSplit = inputPartition -> forkJoinPool.submit(() -> {
                try (VastColumnarBatchReader reader = (VastColumnarBatchReader) readerFactory.createColumnarReader(inputPartition)) {
                    int splitId = inputPartition.getSplitId();
                    readersForVerifyAllocation.add(reader);
                    Thread.currentThread().setName(format("VastDelete-%s-%s", token, splitId));
                    return iterateReader(pages, token, error, reader, splitId);
                }
                catch (Throwable e) {
                    LOG.error(format("%s caught exception", token), e);
                    error.set(true);
                    tx.setCommit(false);
                    throw toRuntime(e);
                }
            });
            List<ForkJoinTask<Boolean>> splits = Arrays.stream(inputPartitions).map(queryDataSplit).collect(Collectors.toList());
            processRowIdPages(client, endpointsSupplier, forkJoinPool, pages, tx, token, splits);
        }
        finally {
            forkJoinPool.shutdownNow();
            pages.forEach(FieldVector::close);
            String forAllocationValidation = tokenHolder.map(VastTraceToken::toString).orElse("null");
            readersForVerifyAllocation.forEach(r -> {
                IllegalStateException illegalStateException = CommonVastColumnarBatchReader.verifyBufferAllocation(forAllocationValidation, r.getAllocator());
                if (illegalStateException != null) {
                    throw illegalStateException;
                }
            });
        }
    }

    private void processRowIdPages(VastClient client, Supplier<URI> endpointsSupplier, ForkJoinPool forkJoinPool,
            LinkedBlockingQueue<FieldVector> pages, VastAutocommitTransaction tx, VastTraceToken token, List<ForkJoinTask<Boolean>> splits)
            throws Throwable
    {
        LOG.info("{} Submitted select pages tasks", token);
        while (!isDone(splits)) {
            LOG.debug("{} Select tasks in progress", token);
            deleteSelectedPages(client, endpointsSupplier, tx, pages, token);
        }
        LOG.info("{} Select tasks are done", token);
        Throwable executionFailure = anyFailed(splits);
        forkJoinPool.shutdownNow();
        if (executionFailure != null) {
            LOG.error("{} Process failed with exception", token, executionFailure);
            throw executionFailure;
        }
        else {
            LOG.info("{} Deleting available row id pages", token);
            deleteSelectedPages(client, endpointsSupplier, tx, pages, token);
        }
    }

    private boolean iterateReader(LinkedBlockingQueue<FieldVector> pages, VastTraceToken token, AtomicBoolean error, VastColumnarBatchReader reader, int splitId)
            throws InterruptedException
    {
        while (!error.get() && reader.next()) {
            LOG.debug("Split {}:{} Fetching next page", token, splitId);
            ColumnarBatch columnarBatch = reader.get();
            LOG.debug("Split {}:{} Fetched next page of {} rows and {} columns",
                    token, splitId, columnarBatch.numRows(), columnarBatch.numCols());
            if (columnarBatch.numRows() > 0) {
                FieldVector newVector = transformColumnarBatchToNewUnsignedRowIDVector(reader, columnarBatch);
                LOG.debug("Split {}:{} Trying to put page to Q: {}", token, splitId, newVector);
                pages.put(newVector);
                LOG.debug("Split {}:{} Successfully put page to Q: {}", token, splitId, newVector);
            }
        }
        if (error.get()) {
            LOG.warn("Split {}:{} is exiting because of an error in another split", token, splitId);
            return false;
        }
        else {
            LOG.debug("Split {}:{} is exiting gracefully", token, splitId);
            return true;
        }
    }

    private Batch getBatch(Filter[] filters)
    {
        VastScanBuilder vastScanBuilder = new VastScanBuilder(this.table);
        Predicate[] notPushedPredicates = vastScanBuilder.pushPredicates(v2Adapt.apply(filters));
        if (notPushedPredicates.length > 0)
            throw new IllegalArgumentException(format("Could not push all predicates: %s", Arrays.toString(filters)));
        return vastScanBuilder.build().toBatch();
    }

    @NotNull
    private static VastAutocommitTransaction getTransaction(VastClient client, VastSparkTransactionsManager transactionsManager)
    {
        return VastAutocommitTransaction.wrap(client, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)));
    }

    @NotNull
    private static FieldVector transformColumnarBatchToNewUnsignedRowIDVector(VastColumnarBatchReader reader, ColumnarBatch columnarBatch)
    {
        return copyVectorBuffers((FieldVector) ((ArrowColumnVector) columnarBatch.column(0)).getValueVector(),
                ROW_ID_UINT64_FIELD.createVector(reader.getAllocator()));
    }

    private void deleteSelectedPages(VastClient client, Supplier<URI> endpointsSupplier, VastAutocommitTransaction tx, LinkedBlockingQueue<FieldVector> pages, VastTraceToken token)
    {
        FieldVector fieldVector = null;
        try {
            while ((fieldVector = pages.poll(1, TimeUnit.SECONDS)) != null) {
                LOG.debug("{} Polled next page of {} rows to delete: {}, field: {}", token, fieldVector.getValueCount(), fieldVector.getField(), fieldVector);

                try (VectorSchemaRoot root = new VectorSchemaRoot(ImmutableList.of(ROW_ID_UINT64_FIELD), ImmutableList.of(fieldVector))) {
                    LOG.debug("{} Deleting next page of {} rows to delete: {}, fields: {}", token, root.getRowCount(), root.getSchema(), root.getFieldVectors());
                    client.deleteRows(tx, table.getTableMD().schemaName, table.getTableMD().tableName, root, endpointsSupplier.get(), Optional.empty());
                }
                fieldVector.close();
            }
        }
        catch (InterruptedException | VastException e) {
            throw toRuntime(e);
        }
        finally {
            if (fieldVector != null) {
                fieldVector.close();
            }
            pages.forEach(FieldVector::close);
        }
    }

    private Throwable anyFailed(List<ForkJoinTask<Boolean>> splits)
    {
        Throwable e = null;
        for (ForkJoinTask<Boolean> split : splits) {
            if (split.getException() != null) {
                if (e == null) {
                    e = split.getException();
                }
                else {
                    e.addSuppressed(split.getException());
                }
            }
        }
        return e;
    }

    private boolean isDone(List<ForkJoinTask<Boolean>> splits)
    {
        return splits.stream().allMatch(ForkJoinTask::isDone);
    }

    private Supplier<URI> getClientsSupplier()
    {
        try {
            List<URI> dataEndpoints = NDB.getConfig().getDataEndpoints();
            AtomicInteger serialIndex = new AtomicInteger(0);
            return () -> dataEndpoints.get(serialIndex.getAndIncrement() % dataEndpoints.size());
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
    }

    // never called
    @Override
    public String name()
    {
        return null;
    }

    // never called
    @Override
    public StructType schema()
    {
        return null;
    }

    // never called
    @Override
    public Set<TableCapability> capabilities()
    {
        return null;
    }
}
