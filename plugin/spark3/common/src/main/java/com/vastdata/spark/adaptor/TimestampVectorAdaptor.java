/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.adaptor;

import com.vastdata.client.adaptor.VectorAdaptor;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static spark.sql.catalog.ndb.SparkVectorAdaptorUtil.convertNonMicroTSVectorToMicroTSVector;

public class TimestampVectorAdaptor
        implements VectorAdaptor
{
    private static final Logger LOG = LoggerFactory.getLogger(TimestampVectorAdaptor.class);

    @Override
    public FieldVector adapt(FieldVector vector, Field field, BufferAllocator allocator)
    {
        LOG.debug("Adapting Timestamp column: {}", field);
        try {
            Field newField = new Field(field.getName(), new FieldType(field.getFieldType().isNullable(), new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), null), null);
            TimeStampMicroTZVector newVector = new TimeStampMicroTZVector(newField, allocator);
            convertNonMicroTSVectorToMicroTSVector((TimeStampVector) vector, newVector);
            return newVector;
        } finally {
            vector.close();
        }
    }
}
