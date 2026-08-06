/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.spark.predicate.VastPredicate;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class VastPITBatch
        extends VastBatch
{
    public static final StructField EST_ROW_COUNT = new StructField(
            "est_row_count", DataTypes.LongType, false, Metadata.empty());
    private static final Logger LOG = LoggerFactory.getLogger(
            VastPITBatch.class);

    public VastPITBatch(VastPITTable table, StructType scanSchema,
            List<List<VastPredicate>> predicates)
    {
        super(table, scanSchema, null, predicates);
        LOG.info("new VastPITBatch: table={}, scanSchema={}, predicates={}",
                table, schema, predicates);
    }

    @Override
    public PartitionReaderFactory createReaderFactory()
    {
        LOG.info("Creating createReaderFactory with schema: {}", schema);
        // PIT must be scanned in a separate transaction.
        // Must pass null transaction in order not to reuse a session transaction if present
        return new VastPartitionReaderFactory(null, batchID, vastConfig,
                table.getTableMD().schemaName, table.getTableMD().tableName,
                schema, limit, predicates, null);
    }

    @Override
    public InputPartition[] planInputPartitions()
    {
        return new InputPartition[] {SINGLE_SPLIT_INPUT_PARTITION};
    }
}
