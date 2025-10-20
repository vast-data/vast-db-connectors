/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import spark.sql.catalog.ndb.TypeUtil;

import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.lang.String.format;

public class EmptyBatchSupplier extends AbstractEmptyBatchSupplier<ColumnarBatch, ColumnVector>
        implements PartitionReader<ColumnarBatch>
{
    private static final Function<FieldVector, ColumnVector> vectorFunction = ArrowColumnVector::new;
    private static final BiFunction<VectorSchemaRoot, Function<FieldVector, ColumnVector>, ColumnarBatch > resultFunction =
            (root, vectorMapper) -> new ColumnarBatch(
                    root.getFieldVectors().stream().map(vectorMapper).toArray(ColumnVector[]::new),
                    0);


    public EmptyBatchSupplier(StructType schema, InputPartition partition) {
        super(format("%s(%s)", EmptyBatchSupplier.class.getSimpleName(), partition.toString()),
                new Schema(TypeUtil.sparkSchemaToArrowFieldsList(schema)),
                vectorFunction, resultFunction);
    }

    @Override
    public boolean next()
    {
        return super.hasNext();
    }

    @Override
    public ColumnarBatch get()
    {
        return supply();
    }

    @Override
    public void close()
            throws IOException
    {
    }
}
