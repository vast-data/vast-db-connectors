/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.spark.FullSliceExtractor;
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
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.sql.catalog.ndb.TypeUtil;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static java.lang.String.format;

public class VastDataWriteFactory
        implements DataWriterFactory
{
    static final Function<VastConfig, VastClient> VAST_CLIENT_SUPPLIER_FROM_SPARK_CONTEXT = vastConfig -> {
        try {
            return NDB.getVastClient(vastConfig);
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
    };

    private static final Logger FACTORY_LOG = LoggerFactory.getLogger(VastDataWriteFactory.class);
    private static final Logger DATA_WRITER_LOG = LoggerFactory.getLogger(VastDataWriter.class);

    private final List<URI> endpoints;
    private final VastTransaction tx;
    private final VastConfig vastConfig;
    private final VastTableMetaData vastTableMetaData;
    private final String vastTraceTokenStr;

    private class VastDataWriter implements DataWriter<InternalRow>, CompletedWriteExecutionComponent
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
        private final int chunkSize;
        private final FullSliceExtractor fullSliceExtractor;

        private VastDataWriter(int dataWriterIndex, Object traceObj)
        {
            this.dataWriteTraceToken = format("(%s:%s:%s)", vastTraceTokenStr, traceObj, dataWriterIndex);
            this.dataWriterIndex = dataWriterIndex;
            this.bgTaskPhasesCompletionListener = new AwaitableCompletionListener(2); // 2 phases - this, BGInserter
            this.bgTaskPhasesCompletionListener.registerFailureAction(() -> {
                DATA_WRITER_LOG.info("{} Rolling back tx: {}", dataWriteTraceToken, tx);
                VastClient vastClient = NDB.getVastClient(vastConfig);
                vastClient.rollbackTransaction(tx);
                return null;
            });
            this.status = new Status(true, null);
            this.writerAllocator = VastArrowAllocator.writeAllocator().newChildAllocator(format("VastDataWriter%s", this.dataWriteTraceToken), 0, Long.MAX_VALUE);
            this.tableArrowSchema = new Schema(TypeUtil.sparkSchemaToArrowFieldsList(vastTableMetaData.schema));
            this.fullSliceExtractor = new FullSliceExtractor(this.tableArrowSchema);
            this.chunkSize = vastConfig.getMaxRowsPerInsert();
            int ordinal = ordinal();
            this.insertArrowVectorsQ = new FunctionalQ<>(VectorSchemaRoot.class, this.dataWriteTraceToken, ordinal, 100, 2, this.bgTaskPhasesCompletionListener);

            ordinal++;
            URI endpoint = endpoints.get(dataWriterIndex % endpoints.size());
            VastBGWriter vastBgWriter = VastBGWriterFactory.forInsert(ordinal, VAST_CLIENT_SUPPLIER_FROM_SPARK_CONTEXT, this.dataWriteTraceToken, vastConfig, endpoint, tx,
                    vastTableMetaData.schemaName, vastTableMetaData.tableName, this.insertArrowVectorsQ, vastTableMetaData.forImportData);
            DATA_WRITER_LOG.info("VastDataWriter{}: INSERT chunkSize = {}, tableArrowSchema = {}", dataWriteTraceToken, chunkSize, tableArrowSchema);
            vastBgWriter.registerCompletionListener(this.bgTaskPhasesCompletionListener);

            this.executorService = Executors.newFixedThreadPool(2, new ThreadFactoryBuilder().setNameFormat("insert-worker-" + dataWriterIndex + "-%s").build());
            executorService.submit(vastBgWriter);
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
            try {
                arrowWriter.write(internalRow);
            }
            catch (RuntimeException re) {
                DATA_WRITER_LOG.error(format("VastDataWriter%s: Exception during arrow write of row no. %s: %s", dataWriteTraceToken, ctr, internalRow), re);
                arrowWriter.finish();
                currentRoot.close();
                throw re;
            }
        }

        private void setNextArrowWriter()
        {
            currentRoot = VectorSchemaRoot.create(tableArrowSchema, this.writerAllocator);
            try {
                arrowWriter = TypeUtil.getArrowSchemaWriter(currentRoot);
            }
            catch (Exception any) {
                DATA_WRITER_LOG.error(format("VastDataWriter%s: Failed creating new writer, ctr = %s", dataWriteTraceToken, ctr), any);
                throw toRuntime(any);
            }
        }

        public void submitInsertChunk()
        {
            try {
                arrowWriter.finish();
            }
            catch (RuntimeException re) {
                currentRoot.close();
                throw re;
            }

            try {
                DATA_WRITER_LOG.info("VastDataWriter{}: Submitting next chunk of {} rows, hash={}", dataWriteTraceToken, currentRoot.getRowCount(), currentRoot.hashCode());
                this.insertArrowVectorsQ.accept(fullSliceExtractor.apply(currentRoot));
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
            DATA_WRITER_LOG.info("VastDataWriter{} commit(), ctr = {}", dataWriteTraceToken, ctr);
            this.bgTaskPhasesCompletionListener.assertFailure();
            if (ctr % chunkSize != 0) {
                submitInsertChunk();
            }
            this.bgTaskPhasesCompletionListener.completed(this);
            try {
                this.bgTaskPhasesCompletionListener.await();
            }
            catch (InterruptedException e) {
                throw new IOException(format("VastDataWriter%s Interrupted while waiting for BG tasks completion", dataWriteTraceToken), e);
            }
            DATA_WRITER_LOG.debug("VastDataWriter{} BG tasks threadpool shutdown", dataWriteTraceToken);
            terminateBackgroundProcesses();
            return new VastCommitMessage(dataWriterIndex);
        }

        @Override
        public void abort()
        {
            DATA_WRITER_LOG.info("VastDataWriter{} abort()", dataWriteTraceToken);
            this.status = new Status(false, null);
            this.bgTaskPhasesCompletionListener.completed(this);
            terminateBackgroundProcesses();
        }

        private void terminateBackgroundProcesses()
        {
            if (!this.executorService.shutdownNow().isEmpty()) {
                try {
                    DATA_WRITER_LOG.info("VastDataWriter{} abort() awaitTermination - start", dataWriteTraceToken);
                    boolean termination = this.executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
                    DATA_WRITER_LOG.info("VastDataWriter{} abort() awaitTermination - end: {}", dataWriteTraceToken, termination);
                }
                catch (InterruptedException e) {
                    if (Thread.interrupted()) {
                        throw new RuntimeException(format("VastDataWriter%s Interrupted while awaiting BG tasks termination", dataWriteTraceToken), e);
                    }
                }
            }
        }

        @Override
        public void close()
        {
            DATA_WRITER_LOG.info("VastDataWriter{} close()", dataWriteTraceToken);
            if (!this.executorService.shutdownNow().isEmpty()) {
                DATA_WRITER_LOG.warn("VastDataWriter{} Data write is closed without successfully terminating background threads", dataWriteTraceToken);
            }
            VectorSchemaRoot tmp;
            while ((tmp = this.insertArrowVectorsQ.get()) != null) {
                DATA_WRITER_LOG.warn("VastDataWriter{} Closing leftover chunk of {} rows: {}", dataWriteTraceToken, tmp.getRowCount(), tmp.hashCode());
                tmp.close();
            }
            this.bgTaskPhasesCompletionListener.assertFailure();
            if (currentRoot != null) {
                currentRoot.close();
            }
            long allocated = this.writerAllocator.getAllocatedMemory();
            if (allocated != 0) {
                String msg = format("VastDataWriter%s: %s bytes are not freed: %s", dataWriteTraceToken, allocated, writerAllocator.toVerboseString());
                DATA_WRITER_LOG.error(msg);
                throw new IllegalStateException(msg); // TODO: consider disabling via config/session
            }
            this.writerAllocator.close();
        }

        @Override
        public String name()
        {
            return format("VastDataWriter%s", dataWriteTraceToken);
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


    public VastDataWriteFactory(VastTransaction tx, VastConfig vastConfig, VastTable vastTable, List<URI> dataEndpoints)
            throws VastUserException
    {
        this.tx = tx;
        this.vastConfig = vastConfig;
        this.vastTraceTokenStr = tx.generateTraceToken(Optional.empty()).toString();
        this.vastTableMetaData = vastTable.getTableMD();
        this.endpoints = dataEndpoints;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId)
    {
        VastDataWriter vastDataWriter = new VastDataWriter(partitionId, taskId);
        FACTORY_LOG.info("Created new writer: {} for partitionId={}, taskId={}", vastDataWriter.name(), partitionId, taskId);
        return vastDataWriter;
    }
}
