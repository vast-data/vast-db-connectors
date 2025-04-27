/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.spark.predicate.VastPredicate;
import com.vastdata.spark.tx.VastAutocommitTransaction;
import com.vastdata.spark.tx.VastSimpleTransactionFactory;
import com.vastdata.spark.tx.VastSparkTransactionsManager;
import ndb.NDB;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.spark.AlwaysFalseFilterUtil.isAlwaysFalsePredicate;

public class VastPartitionReaderFactory
        implements PartitionReaderFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(VastPartitionReaderFactory.class);
    private final String schemaName;
    private final String tableName;
    private final StructType schema;
    private final Integer limit;
    private List<List<VastPredicate>> predicates;
    private final VastSchedulingInfo schedulingInfo;
    private final VastConfig vastConfig;
    private final SimpleVastTransaction tx;
    private boolean forAlter = false;
    private final int batchID;

    public VastPartitionReaderFactory(SimpleVastTransaction tx, int batchID, VastConfig vastConfig, String schemaName, String tableName, StructType schema, Integer limit, List<List<VastPredicate>> predicates)
    {
        this.batchID = batchID;
        this.tx = tx;
        this.vastConfig = vastConfig;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.schema = schema;
        this.limit = limit;
        this.predicates = predicates;
        this.schedulingInfo = getSchedInfo(schemaName, tableName);
    }

    private VastSchedulingInfo getSchedInfo(String schemaName, String tableName)
    {
        VastClient vastClient;
        try {
            vastClient = NDB.getVastClient(vastConfig);
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
        VastSparkTransactionsManager transactionsManager = VastSparkTransactionsManager.getInstance(vastClient, new VastSimpleTransactionFactory());
        try (VastAutocommitTransaction tx = VastAutocommitTransaction.wrap(vastClient, () -> transactionsManager.startTransaction(new StartTransactionContext(false, true)))) {
            VastTraceToken traceToken = tx.generateTraceToken(Optional.empty());
            return vastClient.getSchedulingInfo(tx, traceToken, schemaName, tableName);
        }
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition)
    {
        throw new UnsupportedOperationException("Row-based reader is not supported by NDB");
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition)
    {
        if (isAlwaysFalsePredicate(predicates)) {
            LOG.info("{} Returning EmptyBatchSupplier", batchID);
            return new EmptyBatchSupplier(schema, partition);
        }
        else {
            return new VastColumnarBatchReader(tx, batchID, vastConfig, schemaName, tableName,
                    (VastInputPartition) partition, schema, limit, predicates, schedulingInfo, forAlter, Collections.emptyMap());
        }
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition)
    {
        return true;
    }

    public void setForAlter()
    {
        this.forAlter = true;
    }

    void updatePushdownPredicates(List<List<VastPredicate>> predicates)
    {
        LOG.info("{} Updating predicates for table: {}, predicates: {}", batchID, tableName, predicates);
        this.predicates = predicates;
    }
}
