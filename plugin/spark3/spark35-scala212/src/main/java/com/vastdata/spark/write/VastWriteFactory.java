/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.metrics.ByColumnInserterMetrics;
import com.vastdata.client.metrics.RecordBatchSplitterMetrics;
import com.vastdata.client.rowid.RowIDStrategyType;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.spark.VastArrowAllocator;
import com.vastdata.spark.VastTable;
import com.vastdata.spark.VastTableMetaData;
import com.vastdata.spark.write.bg.AwaitableCompletionListener;
import com.vastdata.spark.write.bg.CompletedWriteExecutionComponent;
import com.vastdata.spark.write.bg.FunctionalQ;
import com.vastdata.spark.write.bg.Status;
import com.vastdata.spark.write.bg.VastBGWriter;
import com.vastdata.spark.write.bg.VastBGWriterFactory;
import ndb.ComplexRowIDPredicate;
import ndb.NDB;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.MutableProjection;
import org.apache.spark.sql.catalyst.expressions.V2ExpressionUtils;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.connector.write.DeltaWriterFactory;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;
import spark.sql.catalog.ndb.BoundBucketFunction;
import spark.sql.catalog.ndb.BoundTruncateFunction;
import spark.sql.catalog.ndb.DaysFunction;
import spark.sql.catalog.ndb.HoursFunction;
import spark.sql.catalog.ndb.MonthsFunction;
import spark.sql.catalog.ndb.TypeUtil;
import spark.sql.catalog.ndb.YearsFunction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_DEC128_FIELD;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_INT64_FIELD;
import static com.vastdata.spark.SparkArrowVectorUtil.ROW_ID_SIGNED_ADAPTOR;
import static com.vastdata.spark.SparkArrowVectorUtil.VASTDB_SPARK_INT64_ROW_ID_NONNULL;
import static java.lang.String.format;
import static ndb.NDBSparkSessionExtension.getSessionUser;
import static spark.sql.catalog.ndb.TypeUtil.VAST_ROW_ID_FIELD_SIGNED_FIELD;

public class VastWriteFactory
        implements DeltaWriterFactory
{
    static final Function<VastConfig, VastClient> VAST_CLIENT_SUPPLIER_FROM_SPARK_CONTEXT = vastConfig -> {
        try {
            return NDB.getVastClient(vastConfig);
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
    };

    private static final Logger FACTORY_LOG = LoggerFactory.getLogger(
            VastWriteFactory.class);
    private static final Logger DATA_WRITER_LOG = LoggerFactory.getLogger(
            VastWriter.class);
    private static final ComplexRowIDPredicate rowIDPredicate = new ComplexRowIDPredicate();

    private final List<URI> endpoints;
    private final VastTransaction tx;
    private final VastConfig vastConfig;
    private final VastTableMetaData vastTableMetaData;
    private final String vastTraceTokenStr;
    private final Map<String, String> sessionConfig;
    private final boolean complexRowID;
    private final Set<String> nonUpdatableColumns;
    private final List<Integer> partitionIndices;
    private final List<String> transformNames;
    private final List<Integer> transformArgs;
    private transient RecordBatchSplitterMetrics splitterMetrics;
    private transient ByColumnInserterMetrics insertMetrics;
    private transient ExecutorService ioExecutor;
    private transient ExecutorService cpuExecutor;

    public VastWriteFactory(VastTransaction tx, VastConfig vastConfig,
            VastTable vastTable, List<URI> dataEndpoints,
            Map<String, String> sessionConfig)
    {
        this.tx = tx;
        this.vastConfig = vastConfig;
        this.vastTraceTokenStr = tx
                .generateTraceToken(Optional.empty())
                .toString();
        this.vastTableMetaData = vastTable.getTableMD();
        this.endpoints = dataEndpoints;
        this.sessionConfig = sessionConfig;
        this.complexRowID = rowIDPredicate.test(vastTable);
        if (vastTable.partitioning() != null && !vastTableMetaData.isForDelete() && !vastTableMetaData.isForUpdate() && !vastTableMetaData.forImportData) {
            this.partitionIndices = Arrays
                    .stream(vastTable.partitioning())
                    .map(t -> Arrays
                            .asList(vastTableMetaData.schema.names())
                            .indexOf(t.references()[0].fieldNames()[0]))
                    .collect(Collectors.toList());
            this.transformNames = Arrays.stream(vastTable.partitioning()).map(
                    Transform::name).collect(Collectors.toList());
            this.transformArgs = Arrays
                    .stream(vastTable.partitioning())
                    .map(t -> t.children().length < 2 ?
                            null :
                            (Integer) ((Literal<?>) t.children()[0]).value())
                    .collect(Collectors.toList());
        }
        else {
            this.partitionIndices = null;
            this.transformNames = null;
            this.transformArgs = null;
        }
        this.nonUpdatableColumns = vastTable.getNonUpdatableColumns();
    }

    @Override
    public DeltaWriter<InternalRow> createWriter(int partitionId, long taskId)
    {
        VastWriter vastDataWriter = new VastWriter(partitionId, taskId);
        FACTORY_LOG.info("Created new writer: {} for partitionId={}, taskId={}",
                vastDataWriter.name(), partitionId, taskId);
        return vastDataWriter;
    }

    private class VastWriter
            implements DeltaWriter<InternalRow>,
            CompletedWriteExecutionComponent
    {
        private final int dataWriterIndex;
        private final String dataWriteTraceToken;
        private final ExecutorService executorService;
        private final AwaitableCompletionListener bgTaskPhasesCompletionListener;
        private final BufferAllocator writerAllocator;
        private final FunctionalQ<VectorSchemaRoot> insertArrowVectorsQ;
        private final Schema tableArrowSchema;
        private final UnaryOperator<VectorSchemaRoot> writeModeAdaptor;
        private final int chunkSize;
        private final QueueCtx defaultCtx;
        private final java.util.Map<InternalRow, QueueCtx> partitionedCtxs;
        private final MutableProjection projector;
        private Status status;

        private VastWriter(int dataWriterIndex, Object traceObj)
        {
            this.dataWriteTraceToken = format("(%s:%s:%s)", vastTraceTokenStr,
                    traceObj, dataWriterIndex);
            this.dataWriterIndex = dataWriterIndex;
            this.bgTaskPhasesCompletionListener = new AwaitableCompletionListener(
                    2); // 2 phases - this, VastBGWriter
            this.bgTaskPhasesCompletionListener.registerFailureAction(() -> {
                DATA_WRITER_LOG.info("VastWriter{} Rolling back tx: {}",
                        dataWriteTraceToken, tx);
                VastClient vastClient = NDB.getVastClient(vastConfig);
                vastClient.rollbackTransaction(tx, null);
                return null;
            });
            this.status = new Status(true, null);
            this.writerAllocator = VastArrowAllocator
                    .writeAllocator()
                    .newChildAllocator(
                            format("VastWriter%s", this.dataWriteTraceToken), 0,
                            Long.MAX_VALUE);
            if (vastTableMetaData.isForDelete()) {
                this.chunkSize = vastConfig.getMaxRowsPerDelete();
                this.writeModeAdaptor = complexRowID ?
                        UnaryOperator.identity() :
                        ROW_ID_SIGNED_ADAPTOR;
                this.tableArrowSchema = new Schema(Lists.newArrayList(
                        complexRowID ?
                                ROW_ID_DEC128_FIELD :
                                ROW_ID_INT64_FIELD));
                DATA_WRITER_LOG.info(
                        "VastWriter{}: DELETE chunkSize = {}, writeSchema = {}",
                        dataWriteTraceToken, chunkSize, tableArrowSchema);
                this.defaultCtx = new QueueCtx(
                        InternalRowsQFactory.forDelete(chunkSize,
                                complexRowID));
                this.partitionedCtxs = null;
                this.projector = null;
            }
            else if (vastTableMetaData.isForUpdate()) {
                this.chunkSize = vastConfig.getMaxRowsPerUpdate();
                if (!complexRowID) {
                    this.writeModeAdaptor = ROW_ID_SIGNED_ADAPTOR;
                    StructField[] fields = vastTableMetaData.schema.fields();
                    StructField[] adaptedFields = new StructField[fields.length];
                    adaptedFields[0] = VAST_ROW_ID_FIELD_SIGNED_FIELD;
                    System.arraycopy(fields, 1, adaptedFields, 1,
                            fields.length - 1);
                    StructType writeSchema = new StructType(adaptedFields);
                    this.tableArrowSchema = new Schema(
                            TypeUtil.sparkSchemaToArrowFieldsList(writeSchema));
                }
                else {
                    this.writeModeAdaptor = UnaryOperator.identity();
                    this.tableArrowSchema = new Schema(
                            TypeUtil.sparkSchemaToArrowFieldsList(
                                    vastTableMetaData.schema));
                }
                DATA_WRITER_LOG.info(
                        "VastWriter{}: UPDATE chunkSize = {}, tableArrowSchema = {}",
                        dataWriteTraceToken, chunkSize, this.tableArrowSchema);
                this.defaultCtx = new QueueCtx(
                        InternalRowsQFactory.forUpdate(chunkSize,
                                complexRowID));
                this.partitionedCtxs = null;
                this.projector = null;
            }
            else {
                this.chunkSize = vastConfig.getMaxRowsPerInsert();
                this.writeModeAdaptor = UnaryOperator.identity();
                StructType writeSchema = vastTableMetaData.schema;
                DATA_WRITER_LOG.info(
                        "VastWriter{}: INSERT chunkSize = {}, writeSchema = {}",
                        dataWriteTraceToken, chunkSize, writeSchema);
                this.tableArrowSchema = new Schema(
                        TypeUtil.sparkSchemaToArrowFieldsList(writeSchema));
                if (partitionIndices != null && vastConfig.getPartitionedInsert()) {
                    List<Expression> projRefs = getTransformExpressions();
                    DATA_WRITER_LOG.info("projector: {}", projRefs);
                    this.projector = MutableProjection.create(
                            JavaConverters.asScalaBuffer(projRefs).toSeq());
                    this.partitionedCtxs = new HashMap<>();
                    this.defaultCtx = null;
                }
                else {
                    this.defaultCtx = new QueueCtx(
                            InternalRowsQFactory.forInsert(chunkSize));
                    this.partitionedCtxs = null;
                    this.projector = null;
                }
            }
            int ordinal = ordinal();
            this.insertArrowVectorsQ = new FunctionalQ<>(VectorSchemaRoot.class,
                    this.dataWriteTraceToken, ordinal, 100, 2,
                    this.bgTaskPhasesCompletionListener);

            ordinal++;
            URI endpoint = endpoints.get(dataWriterIndex % endpoints.size());
            VastBGWriter vastBgWriter = getWriter(ordinal, endpoint);
            vastBgWriter.registerCompletionListener(
                    this.bgTaskPhasesCompletionListener);

            this.executorService = Executors.newFixedThreadPool(2,
                    new ThreadFactoryBuilder()
                            .setNameFormat(
                                    "write-worker-" + dataWriterIndex + "-%s")
                            .build());
            executorService.submit(vastBgWriter);
        }

        private List<Expression> getTransformExpressions()
        {
            return IntStream.range(0, partitionIndices.size()).mapToObj(i -> {
                int idx = partitionIndices.get(i);
                StructField field = vastTableMetaData.schema.apply(idx);
                BoundReference br = new BoundReference(idx, field.dataType(),
                        field.nullable());
                if (transformNames.get(i).startsWith("identity")) {
                    return br;
                }
                else {
                    UnboundFunction uf;
                    if (transformNames.get(i).startsWith("year")) {
                        uf = new YearsFunction();
                    }
                    else if (transformNames.get(i).startsWith("month")) {
                        uf = new MonthsFunction();
                    }
                    else if (transformNames.get(i).startsWith("day")) {
                        uf = new DaysFunction();
                    }
                    else if (transformNames.get(i).startsWith("hour")) {
                        uf = new HoursFunction();
                    }
                    else if (transformNames.get(i).startsWith("bucket")) {
                        uf = new BoundBucketFunction(transformArgs.get(i));
                    }
                    else if (transformNames.get(i).startsWith("truncate")) {
                        int arg = Integer.parseInt(transformNames
                                .get(i)
                                .substring("truncate_".length()));
                        uf = new BoundTruncateFunction(arg);
                    }
                    else {
                        throw new RuntimeException(
                                format("Unsupported transform: %s",
                                        transformNames.get(i)));
                    }
                    ScalarFunction<Integer> boundF = (ScalarFunction<Integer>) uf.bind(
                            new StructType(new StructField[] {field}));
                    return V2ExpressionUtils.resolveScalarFunction(boundF,
                            JavaConverters
                                    .asScalaBuffer(
                                            Arrays.asList((Expression) br))
                                    .toSeq());
                }
            }).collect(Collectors.toList());
        }

        private VastBGWriter getWriter(int ordinal, URI endpoint)
        {
            String endUser = getSessionUser(vastConfig, sessionConfig);
            if (vastTableMetaData.forImportData) {
                return VastBGWriterFactory.forImport(
                        ordinal,
                        VAST_CLIENT_SUPPLIER_FROM_SPARK_CONTEXT,
                        this.dataWriteTraceToken, vastConfig, endpoint, tx,
                        vastTableMetaData.schemaName,
                        vastTableMetaData.tableName, this.insertArrowVectorsQ
                );
            }
            else if (vastTableMetaData.isForUpdate()) {
                return VastBGWriterFactory.forUpdate(ordinal,
                        VAST_CLIENT_SUPPLIER_FROM_SPARK_CONTEXT,
                        this.dataWriteTraceToken, vastConfig, endpoint, tx,
                        vastTableMetaData.schemaName,
                        vastTableMetaData.tableName, this.insertArrowVectorsQ,
                        endUser);
            }
            else if (vastTableMetaData.isForDelete()) {
                return VastBGWriterFactory.forDelete(ordinal,
                        VAST_CLIENT_SUPPLIER_FROM_SPARK_CONTEXT,
                        this.dataWriteTraceToken, vastConfig, endpoint, tx,
                        vastTableMetaData.schemaName,
                        vastTableMetaData.tableName, this.insertArrowVectorsQ,
                        endUser);
            }
            else {
                if (splitterMetrics == null) {
                    splitterMetrics = new RecordBatchSplitterMetrics();
                }
                if (insertMetrics == null) {
                    insertMetrics = new ByColumnInserterMetrics();
                }
                if (ioExecutor == null) {
                    int cores = Integer.parseInt(sessionConfig.getOrElse(
                            "spark.executor.cores", () -> Integer.toString(Runtime.getRuntime().availableProcessors())));
                    ioExecutor = Executors.newFixedThreadPool(
                            Math.min(vastConfig.getNodeIoExecutorNumThreads(), cores),
                            new ThreadFactoryBuilder().setNameFormat("vast-insert-io-%d").build());
                }
                if (cpuExecutor == null) {
                    cpuExecutor = Executors.newFixedThreadPool(
                            2 * Runtime.getRuntime().availableProcessors(),
                            new ThreadFactoryBuilder().setNameFormat("vast-insert-cpu-%d").build());
                }
                return VastBGWriterFactory.forInsert(ordinal,
                        VAST_CLIENT_SUPPLIER_FROM_SPARK_CONTEXT,
                        this.dataWriteTraceToken, vastConfig, endpoints, tx,
                        vastTableMetaData.schemaName,
                        vastTableMetaData.tableName, this.insertArrowVectorsQ, endUser,
                        nonUpdatableColumns,
                        complexRowID ? RowIDStrategyType.DECIMAL_128 : RowIDStrategyType.UNSIGNED_INT64,
                        splitterMetrics,
                        insertMetrics,
                        ioExecutor,
                        cpuExecutor
                );
            }
        }

        private QueueCtx getCtx(InternalRow r)
        {
            if (defaultCtx != null) {
                return defaultCtx;
            }
            InternalRow ir = projector.apply(r);
            return partitionedCtxs.computeIfAbsent(ir.copy(),
                    tr -> new QueueCtx(createRowsQueue()));
        }

        private Queue<InternalRow> createRowsQueue()
        {
            if (vastTableMetaData.isForDelete()) {
                return InternalRowsQFactory.forDelete(chunkSize, complexRowID);
            }

            if (vastTableMetaData.isForUpdate()) {
                return InternalRowsQFactory.forUpdate(chunkSize, complexRowID);
            }

            return InternalRowsQFactory.forInsert(chunkSize);
        }

        private void forAllCtxs(Consumer<QueueCtx> l)
        {
            if (defaultCtx != null) {
                l.accept(defaultCtx);
                return;
            }
            partitionedCtxs.values().forEach(l);
        }

        private int getCtr()
        {
            if (defaultCtx != null) {
                return defaultCtx.getCtr();
            }
            return partitionedCtxs
                    .values()
                    .stream()
                    .mapToInt(QueueCtx::getCtr)
                    .sum();
        }

        @Override
        public void delete(InternalRow metadata, InternalRow id)
                throws IOException
        {
            write(id);
        }

        @Override
        public void update(InternalRow metadata, InternalRow id,
                InternalRow row)
                throws IOException
        {
            long idVal = id.getLong(0);
            long idValFromRow = row.getLong(0); // row id field is the last
            if (idVal != idValFromRow) {
                throw new IllegalStateException(
                        format("VastWriter%s: Value of %s can not be changed: orig id: %s, new id: %s",
                                dataWriteTraceToken,
                                VASTDB_SPARK_INT64_ROW_ID_NONNULL.getName(),
                                idVal, idValFromRow));
            }
            write(row);
        }

        @Override
        public void insert(InternalRow internalRow)
                throws IOException
        {
            write(internalRow);
        }

        private void flushQueues()
        {
            QueueCtx tmp = new QueueCtx(createRowsQueue());
            partitionedCtxs.values().forEach(tmp::steal);
            partitionedCtxs.clear();
            tmp.commit();
        }

        @Override
        public void write(InternalRow internalRow)
                throws IOException
        {
            if (partitionedCtxs != null && partitionedCtxs.size() > vastConfig.getMaxInsertBuckets()) {
                flushQueues();
            }
            bgTaskPhasesCompletionListener.assertFailure();
            getCtx(internalRow).write(internalRow);
        }

        @Override
        public WriterCommitMessage commit()
                throws IOException
        {
            DATA_WRITER_LOG.info("VastWriter{} commit(), ctr = {}",
                    dataWriteTraceToken, getCtr());
            this.bgTaskPhasesCompletionListener.assertFailure();
            if (partitionedCtxs != null) {
                flushQueues();
            }
            else {
                defaultCtx.commit();
            }
            this.bgTaskPhasesCompletionListener.completed(this);
            try {
                this.bgTaskPhasesCompletionListener.await();
            }
            catch (InterruptedException e) {
                throw new IOException(
                        format("VastWriter%s Interrupted while waiting for BG tasks completion",
                                dataWriteTraceToken), e);
            }
            DATA_WRITER_LOG.debug("VastWriter{} BG tasks threadpool shutdown",
                    dataWriteTraceToken);
            terminateBackgroundProcesses();
            return new VastCommitMessage(
                    new WriteCommitInfo(dataWriterIndex, dataWriteTraceToken,
                            getCtr()).toString());
        }

        @Override
        public void abort()
        {
            DATA_WRITER_LOG.info("VastWriter{} abort()", dataWriteTraceToken);
            this.status = new Status(false, null);
            this.bgTaskPhasesCompletionListener.completed(this);
            terminateBackgroundProcesses();
        }

        private void terminateBackgroundProcesses()
        {
            if (!this.executorService.shutdownNow().isEmpty()) {
                try {
                    DATA_WRITER_LOG.info(
                            "VastWriter{} abort() awaitTermination - start",
                            dataWriteTraceToken);
                    boolean termination = this.executorService.awaitTermination(
                            100, TimeUnit.MILLISECONDS);
                    DATA_WRITER_LOG.info(
                            "VastWriter{} abort() awaitTermination - end: {}",
                            dataWriteTraceToken, termination);
                }
                catch (InterruptedException e) {
                    if (Thread.interrupted()) {
                        throw new RuntimeException(
                                format("VastWriter%s Interrupted while awaiting BG tasks termination",
                                        dataWriteTraceToken), e);
                    }
                }
            }
        }

        @Override
        public void close()
        {
            DATA_WRITER_LOG.info("VastWriter{} close()", dataWriteTraceToken);
            if (!this.executorService.shutdownNow().isEmpty()) {
                DATA_WRITER_LOG.warn(
                        "VastWriter{} Data write is closed without successfully terminating background threads",
                        dataWriteTraceToken);
            }
            VectorSchemaRoot tmp;
            while ((tmp = this.insertArrowVectorsQ.get()) != null) {
                DATA_WRITER_LOG.warn(
                        "VastWriter{} Closing leftover chunk of {} rows: {}",
                        dataWriteTraceToken, tmp.getRowCount(), tmp.hashCode());
                tmp.close();
            }
            forAllCtxs(QueueCtx::close);
            this.bgTaskPhasesCompletionListener.assertFailure();
            long allocated = this.writerAllocator.getAllocatedMemory();
            if (allocated != 0) {
                String msg = format("VastWriter%s: %s bytes are not freed: %s",
                        dataWriteTraceToken, allocated,
                        writerAllocator.toVerboseString());
                DATA_WRITER_LOG.error(msg);
                throw new IllegalStateException(
                        msg); // TODO: consider disabling via config/session
            }
            this.writerAllocator.close();
        }

        @Override
        public String name()
        {
            return format("VastWriter%s", dataWriteTraceToken);
        }

        @Override
        public int ordinal()
        {
            return 0;
        }

        @Override
        public Status status()
        {
            return status;
        }

        private class QueueCtx
        {
            private final Queue<InternalRow> rowsQ;
            private int ctr = 0;
            private ArrowWriter arrowWriter;
            private VectorSchemaRoot currentRoot;


            private QueueCtx(Queue<InternalRow> q)
            {
                this.rowsQ = q;
            }

            public void write(InternalRow internalRow)
                    throws IOException
            {
                if (ctr % chunkSize == 0) {
                    setNextArrowWriter();
                }
                writeArrowRow(internalRow);
                if (++ctr % chunkSize == 0) {
                    submitInsertChunk();
                }
            }

            private void writeArrowRow(InternalRow internalRow)
            {
                rowsQ.add(internalRow.copy());
            }

            private void setNextArrowWriter()
            {
                currentRoot = VectorSchemaRoot.create(tableArrowSchema,
                        writerAllocator);
                try {
                    arrowWriter = TypeUtil.getArrowSchemaWriter(currentRoot);
                }
                catch (Exception any) {
                    DATA_WRITER_LOG.error(
                            format("VastWriter%s: Failed creating new writer, ctr = %s",
                                    dataWriteTraceToken, ctr), any);
                    throw toRuntime(any);
                }
            }

            public void submitInsertChunk()
            {
                while (!rowsQ.isEmpty()) {
                    InternalRow internalRow = rowsQ.remove();
                    try {
                        arrowWriter.write(internalRow);
                    }
                    catch (RuntimeException re) {
                        arrowWriter.finish();
                        currentRoot.close();
                        throw new RuntimeException(
                                format("VastWriter%s: Exception during arrow write of row no. %s",
                                        dataWriteTraceToken, ctr), re);
                    }
                }
                try {
                    arrowWriter.finish();
                }
                catch (RuntimeException re) {
                    currentRoot.close();
                    throw re;
                }
                try {
                    DATA_WRITER_LOG.info(
                            "VastWriter{}: Submitting next chunk of {} rows, hash={}: {} ({}, {})",
                            dataWriteTraceToken, currentRoot.getRowCount(),
                            currentRoot.hashCode(), currentRoot.getSchema(),
                            ctr, chunkSize);
                    insertArrowVectorsQ.accept(
                            writeModeAdaptor.apply(currentRoot));
                }
                catch (Throwable any) {
                    currentRoot.close();
                    throw any;
                }
            }

            public void steal(QueueCtx other)
            {
                try {
                    while (!other.rowsQ.isEmpty()) {
                        InternalRow internalRow = other.rowsQ.remove();
                        write(internalRow);
                    }
                    other.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            public void commit()
            {
                if (ctr % chunkSize != 0) {
                    submitInsertChunk();
                }
            }

            public void close()
            {
                if (currentRoot != null) {
                    currentRoot.close();
                }
            }

            private int getCtr()
            {
                return ctr;
            }
        }
    }
}
