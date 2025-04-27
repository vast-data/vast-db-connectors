/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write;

import com.vastdata.ListShuffler;
import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.spark.VastTable;
import com.vastdata.spark.tx.VastAutocommitTransaction;
import com.vastdata.spark.tx.VastSimpleTransactionFactory;
import com.vastdata.spark.tx.VastSparkTransactionsManager;
import ndb.NDB;
import org.apache.spark.sql.connector.write.DeltaBatchWrite;
import org.apache.spark.sql.connector.write.DeltaWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.Optional;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;

public class VastBatchWriter
        implements DeltaBatchWrite
{
    private static final Logger LOG = LoggerFactory.getLogger(VastBatchWriter.class);
    private final VastClient client;
    private final VastSparkTransactionsManager transactionsManager;
    private final VastTable table;
    private VastAutocommitTransaction tx;

    public VastBatchWriter(VastClient client, VastTable table) {
        this.table = table;
        this.client = client;
        this.transactionsManager = VastSparkTransactionsManager.getInstance(client, new VastSimpleTransactionFactory());

    }

    @Override
    public DeltaWriterFactory createBatchWriterFactory(PhysicalWriteInfo info)
    {
        LOG.info("createBatchWriterFactory() number of partitions: {}", info.numPartitions());
        this.tx = VastAutocommitTransaction.wrap(client, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)));
        try {
            ListShuffler<URI> listShuffler = new ListShuffler<>(Optional.ofNullable(NDB.getConfig().getSeedForShufflingEndpoints()));
            return new VastWriteFactory(tx.getTransaction(), NDB.getConfig(), table, listShuffler.randomizeList(NDB.getConfig().getDataEndpoints()));
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message)
    {
        LOG.info("onDataWriterCommit() {}", message);
        DeltaBatchWrite.super.onDataWriterCommit(message);
    }

    @Override
    public void commit(WriterCommitMessage[] messages)
    {
        LOG.info("commit() {}", Arrays.toString(messages));
        if (this.tx != null) {
            try {
                tx.close();
            }
            catch (Exception e) {
                throw toRuntime(e);
            }
        }
        else {
            LOG.warn("Can't commit - No active transaction");
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages)
    {
        LOG.info("abort() {}", Arrays.toString(messages));
        if (this.tx != null) {
            tx.setCommit(false);
            try {
                tx.close();
            }
            catch (Exception e) {
                throw toRuntime(e);
            }
        }
        else {
            LOG.warn("Can't rollback - No active transaction");
        }
    }
}
