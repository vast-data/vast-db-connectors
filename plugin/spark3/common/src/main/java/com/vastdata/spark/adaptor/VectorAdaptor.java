/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.adaptor;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

@FunctionalInterface
public interface VectorAdaptor
{
    FieldVector adapt(FieldVector vector, Field field, BufferAllocator allocator);
}
