/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastUserException;
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
import ndb.NDB;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.connector.write.DeltaWriterFactory;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.sql.catalog.ndb.TypeUtil;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_INT64_FIELD;
import static com.vastdata.spark.SparkArrowVectorUtil.ROW_ID_SIGNED_ADAPTOR;
import static com.vastdata.spark.SparkArrowVectorUtil.VASTDB_SPARK_ROW_ID_NONNULL;
import static java.lang.String.format;
import static spark.sql.catalog.ndb.TypeUtil.SPARK_ROW_ID_SCHEMA;
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

    private static final Logger FACTORY_LOG = LoggerFactory.getLogger(VastWriteFactory.class);
    private static final Logger DATA_WRITER_LOG = LoggerFactory.getLogger(VastWriter.class);

    private final List<URI> endpoints;
    private final VastTransaction tx;
    private final VastConfig vastConfig;
    private final VastTableMetaData vastTableMetaData;
    private final String vastTraceTokenStr;

    private class VastWriter
            implements DeltaWriter<InternalRow>, CompletedWriteExecutionComponent
    {
        private final int dataWriterIndex;
        private final String dataWriteTraceToken;
        private final ExecutorService executorService;
        private final AwaitableCompletionListener bgTaskPhasesCompletionListener;
        private final BufferAllocator writerAllocator;
        private final FunctionalQ<VectorSchemaRoot> insertArrowVectorsQ;
        private Status status;
        private final Schema tableArrowSchema;
        private int ctr = 0;
        private ArrowWriter arrowWriter;
        private VectorSchemaRoot currentRoot;
        private final UnaryOperator<VectorSchemaRoot> writeModeAdaptor;
        private final Queue<InternalRow> rowsQ;
        private final int chunkSize;

        private VastWriter(int dataWriterIndex, Object traceObj)
        {
            this.dataWriteTraceToken = format("(%s:%s:%s)", vastTraceTokenStr, traceObj, dataWriterIndex);
            this.dataWriterIndex = dataWriterIndex;
            this.bgTaskPhasesCompletionListener = new AwaitableCompletionListener(2); // 2 phases - this, VastBGWriter
            this.bgTaskPhasesCompletionListener.registerFailureAction(() -> {
                DATA_WRITER_LOG.info("VastWriter{} Rolling back tx: {}", dataWriteTraceToken, tx);
                VastClient vastClient = NDB.getVastClient(vastConfig);
                vastClient.rollbackTransaction(tx);
                return null;
            });
            this.status = new Status(true, null);
            this.writerAllocator = VastArrowAllocator.writeAllocator().newChildAllocator(format("VastWriter%s", this.dataWriteTraceToken), 0, Long.MAX_VALUE);
            if (vastTableMetaData.isForDelete()) {
                this.chunkSize = vastConfig.getMaxRowsPerDelete();
                this.writeModeAdaptor = ROW_ID_SIGNED_ADAPTOR;
                this.tableArrowSchema = new Schema(Lists.newArrayList(ROW_ID_INT64_FIELD));
                DATA_WRITER_LOG.info("VastWriter{}: DELETE chunkSize = {}, writeSchema = {}", dataWriteTraceToken, chunkSize, SPARK_ROW_ID_SCHEMA);
                this.rowsQ = InternalRowsQFactory.forDelete(chunkSize);
            }
            else if (vastTableMetaData.isForUpdate()) {
                this.chunkSize = vastConfig.getMaxRowsPerUpdate();
                this.writeModeAdaptor = ROW_ID_SIGNED_ADAPTOR;
                StructField[] fields = vastTableMetaData.schema.fields();
                StructField[] adaptedFields = new StructField[fields.length];
                adaptedFields[0] = VAST_ROW_ID_FIELD_SIGNED_FIELD;
                System.arraycopy(fields, 1, adaptedFields, 1, fields.length - 1);
                StructType writeSchema = new StructType(adaptedFields);
                this.tableArrowSchema = new Schema(TypeUtil.sparkSchemaToArrowFieldsList(writeSchema));
                DATA_WRITER_LOG.info("VastWriter{}: UPDATE chunkSize = {}, writeSchema = {}, tableArrowSchema = {}", dataWriteTraceToken, chunkSize, writeSchema, this.tableArrowSchema);
                this.rowsQ = InternalRowsQFactory.forUpdate(chunkSize);
            }
            else {
                this.chunkSize = vastConfig.getMaxRowsPerInsert();
                this.writeModeAdaptor = UnaryOperator.identity();
                StructType writeSchema = vastTableMetaData.schema;
                DATA_WRITER_LOG.info("VastWriter{}: INSERT chunkSize = {}, writeSchema = {}", dataWriteTraceToken, chunkSize, writeSchema);
                this.tableArrowSchema = new Schema(TypeUtil.sparkSchemaToArrowFieldsList(writeSchema));
                this.rowsQ = InternalRowsQFactory.forInsert(chunkSize);
            }
            int ordinal = ordinal();
            this.insertArrowVectorsQ = new FunctionalQ<>(VectorSchemaRoot.class, this.dataWriteTraceToken, ordinal, 100, 2, this.bgTaskPhasesCompletionListener);

            ordinal++;
            URI endpoint = endpoints.get(dataWriterIndex % endpoints.size());
            VastBGWriter vastBgWriter = getWriter(ordinal, endpoint);
            vastBgWriter.registerCompletionListener(this.bgTaskPhasesCompletionListener);

            this.executorService = Executors.newFixedThreadPool(2, new ThreadFactoryBuilder().setNameFormat("write-worker-" + dataWriterIndex + "-%s").build());
            executorService.submit(vastBgWriter);

        }

        private VastBGWriter getWriter(int ordinal, URI endpoint)
        {
            if (vastTableMetaData.forImportData) {
                return VastBGWriterFactory.forInsert(ordinal, VAST_CLIENT_SUPPLIER_FROM_SPARK_CONTEXT, this.dataWriteTraceToken, vastConfig, endpoint, tx,
                    vastTableMetaData.schemaName, vastTableMetaData.tableName, this.insertArrowVectorsQ, true);
            }
            else if (vastTableMetaData.isForUpdate()) {
                return VastBGWriterFactory.forUpdate(ordinal, VAST_CLIENT_SUPPLIER_FROM_SPARK_CONTEXT, this.dataWriteTraceToken, vastConfig, endpoint, tx,
                        vastTableMetaData.schemaName, vastTableMetaData.tableName, this.insertArrowVectorsQ);
            }
            else if (vastTableMetaData.isForDelete()) {
                return VastBGWriterFactory.forDelete(ordinal, VAST_CLIENT_SUPPLIER_FROM_SPARK_CONTEXT, this.dataWriteTraceToken, vastConfig, endpoint, tx,
                        vastTableMetaData.schemaName, vastTableMetaData.tableName, this.insertArrowVectorsQ);
            }
            else {
                return VastBGWriterFactory.forInsert(ordinal, VAST_CLIENT_SUPPLIER_FROM_SPARK_CONTEXT, this.dataWriteTraceToken, vastConfig, endpoint, tx,
                        vastTableMetaData.schemaName, vastTableMetaData.tableName, this.insertArrowVectorsQ, false);
            }
        }

        @Override
        public void delete(InternalRow metadata, InternalRow id)
                throws IOException
        {
            write(id);
        }

        @Override
        public void update(InternalRow metadata, InternalRow id, InternalRow row)
                throws IOException
        {
            long idVal = id.getLong(0);
            long idValFromRow = row.getLong(0); // row id field is the last
            if (idVal != idValFromRow) {
                throw new IllegalStateException(format("VastWriter%s: Value of %s can not be changed: orig id: %s, new id: %s",
                        dataWriteTraceToken, VASTDB_SPARK_ROW_ID_NONNULL.getName(), idVal, idValFromRow));
            }
            write(row);
        }

        @Override
        public void insert(InternalRow internalRow)
                throws IOException
        {
            write(internalRow);
        }

        @Override
        public void write(InternalRow internalRow)
                throws IOException
        {
            bgTaskPhasesCompletionListener.assertFailure();
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
            currentRoot = VectorSchemaRoot.create(tableArrowSchema, this.writerAllocator);
            try {
                arrowWriter = TypeUtil.getArrowSchemaWriter(currentRoot);
            }
            catch (Exception any) {
                DATA_WRITER_LOG.error(format("VastWriter%s: Failed creating new writer, ctr = %s", dataWriteTraceToken, ctr), any);
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
                    throw new RuntimeException(format("VastWriter%s: Exception during arrow write of row no. %s", dataWriteTraceToken, ctr), re);
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
                DATA_WRITER_LOG.info("VastWriter{}: Submitting next chunk of {} rows, hash={}: {}", dataWriteTraceToken, currentRoot.getRowCount(), currentRoot.hashCode(),
                        currentRoot.getSchema());
                this.insertArrowVectorsQ.accept(this.writeModeAdaptor.apply(currentRoot));
            }
            catch (Throwable any) {
                currentRoot.close();
                throw any;
            }
        }

        @Override
        public WriterCommitMessage commit()
                throws IOException
        {
            DATA_WRITER_LOG.info("VastWriter{} commit(), ctr = {}", dataWriteTraceToken, ctr);
            this.bgTaskPhasesCompletionListener.assertFailure();
            if (ctr % chunkSize != 0) {
                submitInsertChunk();
            }
            this.bgTaskPhasesCompletionListener.completed(this);
            try {
                this.bgTaskPhasesCompletionListener.await();
            }
            catch (InterruptedException e) {
                throw new IOException(format("VastWriter%s Interrupted while waiting for BG tasks completion", dataWriteTraceToken), e);
            }
            DATA_WRITER_LOG.debug("VastWriter{} BG tasks threadpool shutdown", dataWriteTraceToken);
            terminateBackgroundProcesses();
            return new VastCommitMessage(dataWriterIndex);
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
                    DATA_WRITER_LOG.info("VastWriter{} abort() awaitTermination - start", dataWriteTraceToken);
                    boolean termination = this.executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
                    DATA_WRITER_LOG.info("VastWriter{} abort() awaitTermination - end: {}", dataWriteTraceToken, termination);
                }
                catch (InterruptedException e) {
                    if (Thread.interrupted()) {
                        throw new RuntimeException(format("VastWriter%s Interrupted while awaiting BG tasks termination", dataWriteTraceToken), e);
                    }
                }
            }
        }

        @Override
        public void close()
        {
            DATA_WRITER_LOG.info("VastWriter{} close()", dataWriteTraceToken);
            if (!this.executorService.shutdownNow().isEmpty()) {
                DATA_WRITER_LOG.warn("VastWriter{} Data write is closed without successfully terminating background threads", dataWriteTraceToken);
            }
            VectorSchemaRoot tmp;
            while ((tmp = this.insertArrowVectorsQ.get()) != null) {
                DATA_WRITER_LOG.warn("VastWriter{} Closing leftover chunk of {} rows: {}", dataWriteTraceToken, tmp.getRowCount(), tmp.hashCode());
                tmp.close();
            }
            if (currentRoot != null) {
                currentRoot.close();
            }
            this.bgTaskPhasesCompletionListener.assertFailure();
            long allocated = this.writerAllocator.getAllocatedMemory();
            if (allocated != 0) {
                String msg = format("VastWriter%s: %s bytes are not freed: %s", dataWriteTraceToken, allocated, writerAllocator.toVerboseString());
                DATA_WRITER_LOG.error(msg);
                throw new IllegalStateException(msg); // TODO: consider disabling via config/session
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
    }


    public VastWriteFactory(VastTransaction tx, VastConfig vastConfig, VastTable vastTable, List<URI> dataEndpoints)
            throws VastUserException
    {
        this.tx = tx;
        this.vastConfig = vastConfig;
        this.vastTraceTokenStr = tx.generateTraceToken(Optional.empty()).toString();
        this.vastTableMetaData = vastTable.getTableMD();
        this.endpoints = dataEndpoints;
    }

    @Override
    public DeltaWriter<InternalRow> createWriter(int partitionId, long taskId)
    {
        VastWriter vastDataWriter = new VastWriter(partitionId, taskId);
        FACTORY_LOG.info("Created new writer: {} for partitionId={}, taskId={}", vastDataWriter.name(), partitionId, taskId);
        return vastDataWriter;
    }
}
