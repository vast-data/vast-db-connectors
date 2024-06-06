/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD_SIGNED;
import static com.vastdata.client.schema.RowIDVectorCopy.copyVectorBuffers;
import static spark.sql.catalog.ndb.TypeUtil.convertFixedSizeBinaryIntoVarchar;
import static spark.sql.catalog.ndb.TypeUtil.convertNonMicroTSVectorToMicroTSVector;

public class FieldToColumnMapper
        implements Function<FieldVector, ColumnVector>
{
    private static final Logger LOG = LoggerFactory.getLogger(FieldToColumnMapper.class);
    private final BufferAllocator allocator;

    public FieldToColumnMapper(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public ColumnVector apply(FieldVector fieldVector)
    {
        Field field = fieldVector.getField();
        if (field.equals(ROW_ID_FIELD)) {
            try {
                FieldVector newVector = copyVectorBuffers(fieldVector, ROW_ID_FIELD_SIGNED.createVector(allocator));
                return new ArrowColumnVector(newVector);
            } finally {
                fieldVector.close();
            }
        }
        else if (field.getType().getTypeID().equals(ArrowType.ArrowTypeID.FixedSizeBinary)) {
            Field newField = new Field(field.getName(), new FieldType(field.getFieldType().isNullable(), ArrowType.Utf8.INSTANCE, null), null);
            VarCharVector dst = new VarCharVector(newField, allocator);
            convertFixedSizeBinaryIntoVarchar((FixedSizeBinaryVector) fieldVector, dst);
            fieldVector.close();
            return new ArrowColumnVector(dst);
        }
        else if (field.getType().getTypeID().equals(ArrowType.ArrowTypeID.Timestamp)) {
            Field newField = new Field(field.getName(), new FieldType(field.getFieldType().isNullable(), new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), null), null);
            TimeStampMicroTZVector microTZVector = new TimeStampMicroTZVector(newField, allocator);
            convertNonMicroTSVectorToMicroTSVector((TimeStampVector) fieldVector, microTZVector);
            fieldVector.close();
            return new ArrowColumnVector(microTZVector);
        }
        else {
            return new ArrowColumnVector(fieldVector);
        }
    }
}
