/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.adaptor;

import com.vastdata.client.adaptor.VectorAdaptor;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static spark.sql.catalog.ndb.SparkVectorAdaptorUtil.convertFixedSizeBinaryIntoVarchar;

public class CharNVectorAdaptor
        implements VectorAdaptor
{
    private static final Logger LOG = LoggerFactory.getLogger(CharNVectorAdaptor.class);

    @Override
    public FieldVector adapt(FieldVector vector, Field field, BufferAllocator allocator)
    {
        LOG.debug("Adapting FixedSizeBinary column: {}", field);
        try {
            Field newField = new Field(field.getName(), new FieldType(field.getFieldType().isNullable(), ArrowType.Utf8.INSTANCE, null), null);
            VarCharVector newVector = new VarCharVector(newField, allocator);
            convertFixedSizeBinaryIntoVarchar((FixedSizeBinaryVector) vector, newVector);
            return newVector;
        } finally {
            vector.close();
        }
    }
}
