/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.vastdata.client.QueryDataPageBuilder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class VastPageBuilder
        implements QueryDataPageBuilder<VectorSchemaRoot>
{
    private static final int DEFAULT_BATCHES_CAPACITY = 4;

    private final Schema requestedSchema;
    private final BufferAllocator allocator;

    private final List<VectorSchemaRoot> batches;

    public VastPageBuilder(Schema requestedSchema, BufferAllocator allocator)
    {
        this.requestedSchema = requestedSchema;
        this.batches = new ArrayList<>(DEFAULT_BATCHES_CAPACITY);
        this.allocator = allocator;
    }

    @Override
    public QueryDataPageBuilder<VectorSchemaRoot> add(VectorSchemaRoot root)
    {
        this.batches.add(root);
        return this;
    }

    @Override
    public VectorSchemaRoot build()
    {
        int rowCount = Math.toIntExact(batches.stream().mapToLong(batch -> (long) batch.getRowCount()).sum());
        VectorSchemaRoot result = VectorSchemaRoot.create(requestedSchema, allocator);
        IntStream.range(0, requestedSchema.getFields().size()).forEach(col -> {
            FieldVector resultColumn = result.getVector(col);
            int resultIndex = 0;
            for (VectorSchemaRoot batch : batches) {
                FieldVector sourceColumn = batch.getVector(col);
                for (int i = 0; i < batch.getRowCount(); ++i) {
                    // TODO: can be optimized for scalar types (ORION-91972)
                    resultColumn.copyFromSafe(i, resultIndex++, sourceColumn);
                }
            }
        });
        result.setRowCount(rowCount);
        return result;
    }

    @Override
    public void clear()
    {
        batches.forEach(VectorSchemaRoot::close);
        batches.clear();
    }
}
