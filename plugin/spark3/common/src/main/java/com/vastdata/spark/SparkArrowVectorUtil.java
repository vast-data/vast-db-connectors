/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD_NAME;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_INT64_FIELD;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_UINT64_FIELD;
import static com.vastdata.client.schema.RowIDVectorCopy.copyVectorBuffers;

public final class SparkArrowVectorUtil
{
    public static final String VASTDB_SPARK_INT64_ROW_ID_COLUMN_NAME = "vastdb_spark_int64_row_id";
    public static final Field VASTDB_SPARK_INT64_ROW_ID_NONNULL = Field.notNullable(
            VASTDB_SPARK_INT64_ROW_ID_COLUMN_NAME, new ArrowType.Int(64, true));
    public static final Field VASTDB_SPARK_DEC128_ROW_ID_NONNULL = Field.notNullable(
            ROW_ID_FIELD_NAME, new ArrowType.Decimal(38, 0, 128));
    public static final UnaryOperator<FieldVector> VAST_ROW_ID_TO_SPARK_ROW_ID_VECTOR_ADAPTOR = f -> {
        if (ROW_ID_FIELD_NAME.equals(f.getName())) {
            FieldVector newRowIDVector;
            if (f.getField().getType().equals(
                    VASTDB_SPARK_DEC128_ROW_ID_NONNULL.getType())) {
                newRowIDVector = copyVectorBuffers(f, new DecimalVector(
                        VASTDB_SPARK_DEC128_ROW_ID_NONNULL.getName(),
                        f.getAllocator(), 38, 0));
            }
            else {
                newRowIDVector = copyVectorBuffers(f, new BigIntVector(
                        VASTDB_SPARK_INT64_ROW_ID_NONNULL.getName(),
                        f.getAllocator()));
            }
            f.close();
            return newRowIDVector;
        }
        else {
            return f;
        }
    };
    private static final UnaryOperator<FieldVector> ROW_ID_SIGNED_TO_UNSIGNED_VECTOR_ADAPTOR = new ConditionalArrowVectorAdaptor(
            ROW_ID_INT64_FIELD.getName(), ROW_ID_UINT64_FIELD::createVector);
    public static final UnaryOperator<VectorSchemaRoot> ROW_ID_SIGNED_ADAPTOR = s -> {
        Schema adaptedSchema = new Schema(
                s.getSchema().getFields().stream().map(f -> {
                    if (ROW_ID_INT64_FIELD.getName().equals(
                            f.getName()) && VASTDB_SPARK_INT64_ROW_ID_NONNULL
                            .getType()
                            .equals(f.getType())) {
                        return ROW_ID_UINT64_FIELD;
                    }
                    else {
                        return f;
                    }
                }).collect(Collectors.toList()));
        return new VectorSchemaRoot(adaptedSchema, s
                .getFieldVectors()
                .stream()
                .map(ROW_ID_SIGNED_TO_UNSIGNED_VECTOR_ADAPTOR)
                .collect(Collectors.toList()), s.getRowCount());
    };

    private SparkArrowVectorUtil()
    {
    }

    private static class ConditionalArrowVectorAdaptor
            implements UnaryOperator<FieldVector>
    {
        private final String fieldNameToMatch;
        private final Function<BufferAllocator, FieldVector> newVectorSupplier;

        private ConditionalArrowVectorAdaptor(String fieldNameToMatch,
                Function<BufferAllocator, FieldVector> newVectorSupplier)
        {
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
                FieldVector newRowIDVector = copyVectorBuffers(fieldVector,
                        newVectorSupplier.apply(fieldVector.getAllocator()));
                fieldVector.close();
                return newRowIDVector;
            }
        }
    }
}
