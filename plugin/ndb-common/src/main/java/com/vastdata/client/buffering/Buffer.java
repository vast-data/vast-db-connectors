/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.buffering;

import com.vastdata.client.buffering.BufferedDml.Config;
import com.vastdata.client.bycolumninserter.SerializedSizeApproximator;
import com.vastdata.client.metrics.BufferedInsertMetrics;
import io.airlift.log.Logger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Buffer
        implements AutoCloseable
{
    private static final Logger LOG = Logger.get(Buffer.class);
    private final Config config;

    private final BufferedInsertMetrics metrics;
    private final BufferAllocator allocator;
    private final List<VectorSchemaRoot> fullVsrs;
    private VectorSchemaRoot openVsr;
    private int rowCount;

    public Buffer(BufferedInsertMetrics metrics, BufferAllocator allocator,
            Config config)
    {
        this.metrics = metrics;

        metrics.incBufferCreated();

        this.fullVsrs = new ArrayList<>();
        this.openVsr = null;
        this.rowCount = 0;
        this.allocator = allocator.newChildAllocator("buffer", 0,
                Long.MAX_VALUE);
        this.config = config;
    }

    public synchronized void add(VsrAppender vsrAppender)
    {
        if (openVsr == null) {
            openVsr = VectorSchemaRoot.create(vsrAppender.getSchema(),
                    allocator);
            for (FieldVector vector : openVsr.getFieldVectors()) {
                vector.setInitialCapacity(
                        config.getBufferOpenVsrRowCountPreallocation());
                vector.allocateNew();
            }
            openVsr.setRowCount(0);
            metrics.incBufferVsrAdded();
        }

        rowCount += vsrAppender.getRowCount();
        vsrAppender.append(openVsr);

        metrics.recordBufferRowCount(rowCount);

        if (openVsr.getRowCount() >= config.getBufferOpenVsrTargetRowCount()) {
            fullVsrs.add(openVsr);
            LOG.debug(
                    "ADDING open to full vsrs, closedVsrsCount: %d, rowCountTotal: %d",
                    fullVsrs.size(), rowCount);
            openVsr = null;
        }
    }

    public synchronized int getRowCount()
    {
        return rowCount;
    }

    public synchronized Long approximateSerializedBytes()
    {
        long sum = fullVsrs
                .stream()
                .mapToLong(SerializedSizeApproximator::approximateSize)
                .sum();
        if (openVsr != null) {
            sum += SerializedSizeApproximator.approximateSize(openVsr);
        }
        return sum;
    }

    public synchronized int getVsrCount()
    {
        return fullVsrs.size() + (openVsr != null ? 1 : 0);
    }

    public synchronized List<VectorSchemaRoot> moveVsrs(
            BufferAllocator targetAllocator)
    {
        if (openVsr != null) {
            fullVsrs.add(openVsr);
            this.openVsr = null;
        }

        List<VectorSchemaRoot> result = fullVsrs
                .stream()
                .map(vsr -> Tools.transferVsr(targetAllocator, vsr))
                .collect(Collectors.toList());

        metrics.recordBufferVsrRemoved(fullVsrs.size());

        fullVsrs.clear();
        return result;
    }

    @Override
    public synchronized void close()
    {
        metrics.incBufferClosed();

        if (openVsr != null) {
            openVsr.close();
            openVsr = null;
            metrics.recordBufferVsrRemoved(1);
        }

        fullVsrs.forEach(VectorSchemaRoot::close);

        allocator.close();
    }
}
