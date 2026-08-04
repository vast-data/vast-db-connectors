/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.bycolumninserter;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

class ExecutionContext
        implements AutoCloseable
{
    private final BufferAllocator workAllocator;

    // Step 1 Result
    private List<byte[]> insertPayloads;
    private List<VectorSchemaRoot> updateVsrs;

    // Step 2 Result
    private VectorSchemaRoot insertRowIds;

    // Step 3 Result
    private List<byte[]> updatePayloads;

    public ExecutionContext(BufferAllocator workAllocator)
    {
        this.workAllocator = workAllocator;
    }

    public BufferAllocator getWorkAllocator()
    {
        return workAllocator;
    }

    public List<byte[]> takeInsertPayloads()
    {
        List<byte[]> payloads = insertPayloads;
        insertPayloads = null;
        return payloads;
    }

    public List<VectorSchemaRoot> getUpdateVsrs()
    {
        return updateVsrs;
    }

    public void stepOneFinish(List<byte[]> insertPayloads,
            List<VectorSchemaRoot> updateVsrs)
    {
        this.insertPayloads = insertPayloads;
        this.updateVsrs = updateVsrs;
    }

    public VectorSchemaRoot getInsertRowIds()
    {
        return insertRowIds;
    }

    public void stepTwoFinish(VectorSchemaRoot insertRowIds)
    {
        this.insertRowIds = insertRowIds;
    }

    public List<byte[]> takeUpdatePayloads()
    {
        List<byte[]> payloads = updatePayloads;
        updatePayloads = null;
        return payloads;
    }

    public void stepThreeFinish(List<byte[]> updatePayloads)
    {
        this.updateVsrs.forEach(VectorSchemaRoot::close);
        this.updateVsrs.clear();
        this.updatePayloads = updatePayloads;
    }

    @Override
    public void close()
    {
        if (updateVsrs != null) {
            updateVsrs.forEach(VectorSchemaRoot::close);
            updateVsrs.clear();
        }
        workAllocator.close();
    }
}
