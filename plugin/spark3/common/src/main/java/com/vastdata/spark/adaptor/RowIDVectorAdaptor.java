/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.adaptor;

import com.vastdata.client.adaptor.VectorAdaptor;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_INT64_FIELD;
import static com.vastdata.client.schema.RowIDVectorCopy.copyVectorBuffers;

public class RowIDVectorAdaptor
        implements VectorAdaptor
{
    private static final Logger LOG = LoggerFactory.getLogger(RowIDVectorAdaptor.class);

    @Override
    public FieldVector adapt(FieldVector vector, Field field, BufferAllocator allocator)
    {
        LOG.debug("Adapting ROW_ID column");
        try {
            return copyVectorBuffers(vector, ROW_ID_INT64_FIELD.createVector(allocator));
        } finally {
            vector.close();
        }
    }
}
