/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.buffering;

import com.vastdata.client.buffering.BufferedDml.Config;
import com.vastdata.client.metrics.BufferedInsertMetrics;
import io.airlift.log.Logger;
import org.apache.arrow.memory.BufferAllocator;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class BufferMap
{
    private static final Logger LOG = Logger.get(BufferedDml.class);

    private final BufferedInsertMetrics metrics;
    private final ConcurrentMap<Long, BufferHandle> map = new ConcurrentHashMap<>();
    private final BufferAllocator allocator;
    private final Config config;

    public BufferMap(BufferedInsertMetrics metrics, BufferAllocator allocator,
            Config config)
    {
        this.metrics = metrics;
        this.allocator = allocator;
        this.config = config;
    }

    public Optional<Buffer> putAndRemoveIfFull(Long bufferId,
            VsrAppender vsrAppender)
    {
        while (true) {
            BufferHandle handle = map.computeIfAbsent(bufferId,
                    k -> new BufferHandle(metrics, allocator, config));

            synchronized (handle) {
                if (handle.isExtracted()) {
                    continue;
                }

                handle.buffer.add(vsrAppender);

                if (handle.buffer.getRowCount() >= config.getTargetRowCountPerPartitionFlush()) {
                    if (map.remove(bufferId, handle)) {
                        handle.setExtracted();
                        LOG.debug("Outgoing (full-buffer) rowCount: %d",
                                handle.buffer.getRowCount());
                        return Optional.of(handle.buffer);
                    }
                    // if remove has failed, another thread has removed and is responsible to flush it.
                    // the new rows were appended
                }

                return Optional.empty();
            }
        }
    }

    public List<Buffer> extractFullBuffers()
    {
        List<Buffer> extractedBuffers = new ArrayList<>();

        for (Map.Entry<Long, BufferHandle> entry : map.entrySet()) {
            BufferHandle handle = entry.getValue();

            if (handle.buffer.getRowCount() < config.getBufferOpenVsrTargetRowCount()) {
                continue;
            }

            synchronized (handle) {
                if (handle.isExtracted()) {
                    continue;
                }

                if (handle.buffer.getRowCount() >= config.getBufferOpenVsrTargetRowCount()) {
                    if (map.remove(entry.getKey(), handle)) {
                        handle.setExtracted();
                        extractedBuffers.add(handle.buffer);
                    }
                }
            }
        }

        LOG.debug("extracted n=%s full-buffers", extractedBuffers.size());
        if (LOG.isDebugEnabled()) {
            int rowCountSum = extractedBuffers
                    .stream()
                    .mapToInt(Buffer::getRowCount)
                    .sum();
            LOG.debug("Outgoing (full-buffers) rowCount: %d", rowCountSum);
        }
        return extractedBuffers;
    }

    public List<Buffer> extractLargestBuffers(long targetSize)
    {
        LOG.debug("extracting largest buffers. target-size: %d", targetSize);

        Comparator<SimpleEntry<Long, Integer>> cmp = Comparator.comparingLong(
                Map.Entry::getValue);
        List<Long> bufferIdsBySize = map
                .entrySet()
                .stream()
                .map(e -> new SimpleEntry<>(e.getKey(),
                        e.getValue().buffer.getRowCount()))
                .sorted(cmp.reversed())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        List<Buffer> extractedBuffers = new ArrayList<>();
        long totalApproxSize = 0;

        for (Long bufferId : bufferIdsBySize) {
            BufferHandle handle = map.get(bufferId);

            if (handle == null) {
                continue;
            }

            synchronized (handle) {
                if (handle.isExtracted() || handle.buffer.getRowCount() == 0) {
                    continue;
                }

                long approxSize = handle.buffer.approximateSerializedBytes();

                if (totalApproxSize == 0 || totalApproxSize + approxSize <= targetSize) {
                    if (map.remove(bufferId, handle)) {
                        handle.setExtracted();
                        extractedBuffers.add(handle.buffer);
                        totalApproxSize += approxSize;
                    }
                }
                else {
                    if (totalApproxSize > 0) {
                        break;
                    }
                }
            }
        }

        LOG.debug("extracted n=%s buffers of size %s bytes",
                extractedBuffers.size(), totalApproxSize);
        if (LOG.isDebugEnabled()) {
            int rowCountSum = extractedBuffers
                    .stream()
                    .mapToInt(Buffer::getRowCount)
                    .sum();
            LOG.debug("Outgoing (largest-buffers) rowCount: %d", rowCountSum);
        }

        return extractedBuffers;
    }

    public List<Integer> getVsrCounts()
    {
        return map
                .values()
                .stream()
                .map(handle -> handle.buffer.getVsrCount())
                .collect(Collectors.toList());
    }

    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    public void close()
    {
        map.values().forEach(BufferHandle::close);
    }

    private static class BufferHandle
    {
        final Buffer buffer;
        private boolean extracted;

        BufferHandle(BufferedInsertMetrics metrics, BufferAllocator allocator,
                Config config)
        {
            this.buffer = new Buffer(metrics, allocator, config);
            this.extracted = false;
        }

        public boolean isExtracted()
        {
            return extracted;
        }

        public void setExtracted()
        {
            extracted = true;
        }

        public void close()
        {
            buffer.close();
        }
    }
}
