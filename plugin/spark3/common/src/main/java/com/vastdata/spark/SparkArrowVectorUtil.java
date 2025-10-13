/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD_SIGNED;
import static com.vastdata.client.schema.RowIDVectorCopy.copyVectorBuffers;

public final class SparkArrowVectorUtil
{
    public static final Field VASTDB_SPARK_ROW_ID_NONNULL = Field.notNullable("vastdb_spark_row_id", new ArrowType.Int(64, true));

    private SparkArrowVectorUtil() {}

    private static class ConditionalArrowVectorAdaptor implements UnaryOperator<FieldVector>
    {
        private final String fieldNameToMatch;
        private final Function<BufferAllocator, FieldVector> newVectorSupplier;

        private ConditionalArrowVectorAdaptor(String fieldNameToMatch, Function<BufferAllocator, FieldVector> newVectorSupplier) {
            this.fieldNameToMatch = fieldNameToMatch;
            this.newVectorSupplier = newVectorSupplier;
        }

        @Override
        public FieldVector apply(FieldVector fieldVector)
        {
            if (!fieldNameToMatch.equals(fieldVector.getName())) {
                return fieldVector;
            }
            else {
                FieldVector newRowIDVector = copyVectorBuffers(fieldVector, newVectorSupplier.apply(fieldVector.getAllocator()));
                fieldVector.close();
                return newRowIDVector;
            }
        }
    }

    public static final UnaryOperator<FieldVector> VAST_ROW_ID_TO_SPARK_ROW_ID_VECTOR_ADAPTOR = new ConditionalArrowVectorAdaptor(ROW_ID_FIELD.getName(), allocator -> new BigIntVector(VASTDB_SPARK_ROW_ID_NONNULL.getName(), allocator));

    private static final UnaryOperator<FieldVector> ROW_ID_SIGNED_TO_UNSIGNED_VECTOR_ADAPTOR = new ConditionalArrowVectorAdaptor(ROW_ID_FIELD_SIGNED.getName(), ROW_ID_FIELD::createVector);
    public static final UnaryOperator<VectorSchemaRoot> ROW_ID_SIGNED_ADAPTOR = s -> new VectorSchemaRoot(s.getFieldVectors().stream().map(ROW_ID_SIGNED_TO_UNSIGNED_VECTOR_ADAPTOR).collect(Collectors.toList()));

}
