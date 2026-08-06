/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.buffering;

import com.vastdata.Pair;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastExceptionFactory;
import com.vastdata.client.metrics.BufferedInsertMetrics;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.TransferPair;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class BufferedRowIdBasedWriteTaskFactory
        implements BufferedTaskFactory
{
    private final ExecutorService ioExecutor;
    private final List<URI> dataEndpoints;

    public BufferedRowIdBasedWriteTaskFactory(List<URI> dataEndpoints,
            ExecutorService ioExecutor)
    {
        this.ioExecutor = ioExecutor;
        this.dataEndpoints = dataEndpoints;
    }

    public static VectorSchemaRoot sortVsrByRowIdColumn(
            VectorSchemaRoot sourceVsr, BufferAllocator allocator)
    {
        int rowCount = sourceVsr.getRowCount();
        if (rowCount == 0) {
            return Tools.transferVsr(allocator, sourceVsr);
        }

        int[] indices = new int[rowCount];
        FieldVector rowIdVector = sourceVsr.getVector(0);

        if (rowIdVector instanceof UInt8Vector) {
            UInt8Vector uint8Vec = (UInt8Vector) rowIdVector;
            @SuppressWarnings(
                    "unchecked") Pair<Integer, Long>[] pairs = new Pair[rowCount];
            for (int i = 0; i < rowCount; i++) {
                pairs[i] = Pair.of(i, uint8Vec.get(i));
            }
            Arrays.sort(pairs,
                    (a, b) -> Long.compare(a.getRight(), b.getRight()));
            for (int i = 0; i < rowCount; i++) {
                indices[i] = pairs[i].getLeft();
            }
        }
        else if (rowIdVector instanceof DecimalVector) {
            DecimalVector decVec = (DecimalVector) rowIdVector;
            @SuppressWarnings(
                    "unchecked") Pair<Integer, java.math.BigDecimal>[] pairs = new Pair[rowCount];
            for (int i = 0; i < rowCount; i++) {
                pairs[i] = Pair.of(i, decVec.getObject(i));
            }
            Arrays.sort(pairs, (a, b) -> a.getRight().compareTo(b.getRight()));
            for (int i = 0; i < rowCount; i++) {
                indices[i] = pairs[i].getLeft();
            }
        }
        else {
            throw new IllegalStateException(
                    "Unsupported row id type: " + rowIdVector
                            .getClass()
                            .getSimpleName());
        }

        VectorSchemaRoot targetVsr = VectorSchemaRoot.create(
                sourceVsr.getSchema(), allocator);
        for (FieldVector vector : targetVsr.getFieldVectors()) {
            vector.setInitialCapacity(rowCount);
            vector.allocateNew();
        }

        for (int i = 0; i < sourceVsr.getFieldVectors().size(); i++) {
            FieldVector sourceVec = sourceVsr.getVector(i);
            FieldVector targetVec = targetVsr.getVector(i);

            VectorCopier copier = createCopier(sourceVec, targetVec);
            for (int j = 0; j < rowCount; j++) {
                copier.copy(indices[j], j);
            }
        }

        targetVsr.setRowCount(rowCount);

        return targetVsr;
    }

    private static VectorCopier createCopier(FieldVector source,
            FieldVector target)
    {
        if (source instanceof ListVector) { // Handles both ListVector and MapVector
            ListVector srcList = (ListVector) source;
            ListVector tgtList = (ListVector) target;
            VectorCopier dataCopier = createCopier(srcList.getDataVector(),
                    tgtList.getDataVector());

            return (from, to) -> {
                if (srcList.isNull(from)) {
                    tgtList.setNull(to);
                    return;
                }
                tgtList.setNotNull(to);
                int start = srcList.getElementStartIndex(from);
                int end = srcList.getElementEndIndex(from);
                int len = end - start;
                int tgtStart = tgtList.startNewValue(to);
                for (int i = 0; i < len; i++) {
                    dataCopier.copy(start + i, tgtStart + i);
                }
                tgtList.endValue(to, len);
            };
        }
        else if (source instanceof StructVector) {
            StructVector srcStruct = (StructVector) source;
            StructVector tgtStruct = (StructVector) target;
            List<VectorCopier> childCopiers = new ArrayList<>();
            for (String name : srcStruct.getChildFieldNames()) {
                childCopiers.add(createCopier(srcStruct.getChild(name),
                        tgtStruct.getChild(name)));
            }

            return (from, to) -> {
                if (srcStruct.isNull(from)) {
                    tgtStruct.setNull(to);
                    return;
                }
                tgtStruct.setIndexDefined(to);
                for (VectorCopier child : childCopiers) {
                    child.copy(from, to);
                }
            };
        }
        else {
            // Base case for primitive vectors (including FixedSizeBinaryVector).
            // Arrow's native TransferPair works perfectly and safely here.
            TransferPair tp = source.makeTransferPair(target);
            return tp::copyValueSafe;
        }
    }

    @Override
    public CompletableFuture<Void> executeAsync(List<Buffer> buffers,
            BufferedInsertMetrics bufferedInsertMetrics,
            BufferAllocator allocator)
    {
        BufferAllocator taskAllocator = allocator.newChildAllocator(
                "rowbased-buffered-task", 0, Long.MAX_VALUE);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicInteger counter = new AtomicInteger(0);

        for (Buffer buffer : buffers) {
            List<VectorSchemaRoot> vsrs = buffer.moveVsrs(taskAllocator);
            buffer.close();
            if (vsrs.isEmpty()) {
                continue;
            }

            int endpointIndex = counter.getAndIncrement();
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try (VectorSchemaRoot mergedVsr = Tools.mergeVsrs(vsrs, taskAllocator);
                        VectorSchemaRoot sortedVsr = sortVsrByRowIdColumn(mergedVsr, taskAllocator)) {
                    sendVsr(sortedVsr, dataEndpoints.get(endpointIndex % dataEndpoints.size()), Optional.empty());
                }
                catch (VastException e) {
                    throw VastExceptionFactory.toRuntime(e);
                }
            }, ioExecutor);
            futures.add(future);
        }

        return CompletableFuture
                .allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((res, err) -> taskAllocator.close());
    }

    protected abstract void sendVsr(VectorSchemaRoot vsr, URI dataEndpoint,
            Optional<Integer> maxRowsPerRpc)
            throws VastException;

    private interface VectorCopier
    {
        void copy(int from, int to);
    }
}
