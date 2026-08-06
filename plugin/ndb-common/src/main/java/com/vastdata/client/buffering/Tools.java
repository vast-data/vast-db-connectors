/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.buffering;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import java.util.List;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class Tools
{
    public static VectorSchemaRoot mergeVsrs(List<VectorSchemaRoot> vsrs,
            BufferAllocator allocator)
    {
        if (vsrs.isEmpty()) {
            throw new RuntimeException(
                    "mergeVsrs should not be called with empty list");
        }

        if (vsrs.size() == 1) {
            return transferVsr(allocator, vsrs.get(0));
        }

        final int totalRowCount = vsrs
                .stream()
                .mapToInt(VectorSchemaRoot::getRowCount)
                .sum();

        VectorSchemaRoot merged = VectorSchemaRoot.create(
                vsrs.get(0).getSchema(), allocator);

        try {
            for (FieldVector vector : merged.getFieldVectors()) {
                vector.setInitialCapacity(totalRowCount);
                vector.allocateNew();
            }

            VectorSchemaRootAppender.append(false, merged,
                    vsrs.toArray(new VectorSchemaRoot[0]));

            merged.setRowCount(totalRowCount);
        }
        catch (Throwable t) {
            merged.close();
            throw t;
        }
        finally {
            vsrs.forEach(VectorSchemaRoot::close);
            vsrs.clear();
        }

        return merged;
    }

    /*
     * takes ownership of sourceVsr (and closes it)
     */
    public static VectorSchemaRoot transferVsr(BufferAllocator targetAllocator,
            VectorSchemaRoot sourceVsr)
    {
        VectorSchemaRoot targetVsr = VectorSchemaRoot.create(
                sourceVsr.getSchema(), targetAllocator);

        for (int i = 0; i < sourceVsr.getFieldVectors().size(); i++) {
            FieldVector sourceVec = sourceVsr.getVector(i);
            FieldVector targetVec = targetVsr.getVector(i);

            TransferPair tp = sourceVec.makeTransferPair(targetVec);
            tp.transfer();
        }

        targetVsr.setRowCount(sourceVsr.getRowCount());
        sourceVsr.close();
        return targetVsr;
    }
}
