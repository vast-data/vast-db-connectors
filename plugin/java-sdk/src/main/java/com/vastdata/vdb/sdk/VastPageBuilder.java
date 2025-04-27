package com.vastdata.vdb.sdk;

import com.vastdata.client.QueryDataPageBuilder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static java.lang.Math.toIntExact;

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
        int rowCount = toIntExact(batches.stream().mapToLong(batch -> (long) batch.getRowCount()).sum());
        VectorSchemaRoot result = VectorSchemaRoot.create(requestedSchema, allocator);
        if (rowCount > 0) {
            IntStream.range(0, requestedSchema.getFields().size()).forEach(col -> {
                FieldVector resultColumn = result.getVector(col);
                resultColumn.setValueCount(rowCount);
                resultColumn.setInitialCapacity(rowCount);
                resultColumn.allocateNew();
            });
            VectorSchemaRootAppender.append(result, batches.toArray(new VectorSchemaRoot[0]));
        }
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
