/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.QueryDataExtraParams;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.spark.predicate.VastPredicate;
import ndb.NDBSparkSessionExtension;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.vastdata.spark.AlwaysFalseFilterUtil.isAlwaysFalsePredicate;

public class VastPartitionReaderFactory
        implements PartitionReaderFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(
            VastPartitionReaderFactory.class);
    private final String schemaName;
    private final String tableName;
    private final StructType schema;
    private final Integer limit;
    private final String endUser;
    private final VastSchedulingInfo schedulingInfo;
    private final VastConfig vastConfig;
    private final SimpleVastTransaction tx;
    private final int batchID;
    private List<List<VastPredicate>> predicates;
    private boolean forAlter = false;

    public VastPartitionReaderFactory(SimpleVastTransaction tx, int batchID,
            VastConfig vastConfig, String schemaName, String tableName,
            StructType schema, Integer limit,
            List<List<VastPredicate>> predicates, VastSchedulingInfo schedInfo)
    {
        this.batchID = batchID;
        this.tx = tx;
        this.vastConfig = vastConfig;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.schema = schema;
        this.limit = limit;
        this.predicates = predicates;
        this.schedulingInfo = schedInfo;
        this.endUser = NDBSparkSessionExtension.getSessionUser(vastConfig);
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition)
    {
        throw new UnsupportedOperationException(
                "Row-based reader is not supported by NDB");
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(
            InputPartition partition)
    {
        if (isAlwaysFalsePredicate(predicates)) {
            LOG.info("{} Returning EmptyBatchSupplier", batchID);
            return new EmptyBatchSupplier(schema, partition);
        }
        else {
            return new VastColumnarBatchReader(tx, batchID, vastConfig,
                    schemaName, tableName, (VastInputPartition) partition,
                    schema, limit, predicates, schedulingInfo, forAlter,
                    new QueryDataExtraParams(), endUser);
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
        LOG.info("{} Updating predicates for table: {}, predicates: {}",
                batchID, tableName, predicates);
        this.predicates = predicates;
    }
}
