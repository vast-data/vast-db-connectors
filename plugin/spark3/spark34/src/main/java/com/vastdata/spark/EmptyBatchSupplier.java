/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.sql.catalog.ndb.TypeUtil;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.lang.String.format;

public class EmptyBatchSupplier
        implements PartitionReader<ColumnarBatch>
{
    private static final Logger LOG = LoggerFactory.getLogger(EmptyBatchSupplier.class);
    private final String logStr;
    private final Schema schema;
    private final AtomicBoolean hasNext = new AtomicBoolean(true);

    public EmptyBatchSupplier(StructType schema, InputPartition partition) {
        this.logStr = format("%s(%s)", this.getClass().getSimpleName(), partition.toString());
        this.schema = new Schema(TypeUtil.sparkSchemaToArrowFieldsList(schema));
    }

    @Override
    public boolean next()
    {
        boolean hasNext = this.hasNext.getAndSet(false);
        LOG.debug("{} hasNext: {}", logStr, hasNext);
        return hasNext;
    }

    @Override
    public ColumnarBatch get()
    {
        RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        Function<FieldVector, ColumnVector> mapper = new FieldToColumnMapper(allocator);
        ColumnarBatch columnarBatch = new ColumnarBatch(root.getFieldVectors().stream().map(mapper).toArray(ColumnVector[]::new), 0);
        LOG.debug("{} Returning empty page: {}", logStr, columnarBatch);
        return columnarBatch;
    }

    @Override
    public void close()
            throws IOException
    {

    }
}
