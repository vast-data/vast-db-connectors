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

import java.util.function.Function;

import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD_SIGNED;
import static com.vastdata.client.schema.RowIDVectorCopy.copyVectorBuffers;
import static spark.sql.catalog.ndb.SparkVectorAdaptorUtil.convertFixedSizeBinaryIntoVarchar;
import static spark.sql.catalog.ndb.SparkVectorAdaptorUtil.convertNonMicroTSVectorToMicroTSVector;
import static spark.sql.catalog.ndb.SparkVectorAdaptorUtil.requiresTSConversion;

public class FieldToColumnMapper<T>
        implements Function<FieldVector, T>
{
    private final BufferAllocator allocator;
    private final Function<FieldVector, T> factory;

    public FieldToColumnMapper(BufferAllocator allocator, Function<FieldVector, T> factory) {
        this.factory = factory;
        this.allocator = allocator;
    }

    @Override
    public T apply(FieldVector fieldVector)
    {
        Field field = fieldVector.getField();
        if (field.equals(ROW_ID_FIELD)) {
            try {
                FieldVector newVector = copyVectorBuffers(fieldVector, ROW_ID_FIELD_SIGNED.createVector(allocator));
                return factory.apply(newVector);
            } finally {
                fieldVector.close();
            }
        }
        else if (field.getType().getTypeID().equals(ArrowType.ArrowTypeID.FixedSizeBinary)) {
            Field newField = new Field(field.getName(), new FieldType(field.getFieldType().isNullable(), ArrowType.Utf8.INSTANCE, null), null);
            VarCharVector dst = new VarCharVector(newField, allocator);
            convertFixedSizeBinaryIntoVarchar((FixedSizeBinaryVector) fieldVector, dst);
            fieldVector.close();
            return factory.apply(dst);
        }
        else if (field.getType().getTypeID().equals(ArrowType.ArrowTypeID.Timestamp) && requiresTSConversion((TimeStampVector) fieldVector)) {
            Field newField = new Field(field.getName(), new FieldType(field.getFieldType().isNullable(), new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), null), null);
            TimeStampMicroTZVector microTZVector = new TimeStampMicroTZVector(newField, allocator);
            convertNonMicroTSVectorToMicroTSVector((TimeStampVector) fieldVector, microTZVector);
            fieldVector.close();
            return factory.apply(microTZVector);
        }
        else {
            return factory.apply(fieldVector);
        }
    }
}
