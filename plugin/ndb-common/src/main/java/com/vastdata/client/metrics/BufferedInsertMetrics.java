/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.metrics;

import org.weakref.jmx.Managed;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

public class BufferedInsertMetrics
        implements VastMetrics<BufferedInsertMetrics>
{
    protected final RunningStats partitionInserts = new RunningStats();

    private final AtomicLong insertBufferAllocatedMemory = new AtomicLong(0);

    private final LongAdder flushFullPartitionCount = new LongAdder();
    private final LongAdder flushFewPartitionsTogetherCount = new LongAdder();

    private final LongAdder flushAllNanos = new LongAdder();
    private final LongAdder flushAllCount = new LongAdder();

    private final LongAdder maybeFlushSomeNanos = new LongAdder();
    private final LongAdder maybeFlushSomeCount = new LongAdder();

    private final LongAdder sendNanos = new LongAdder();
    private final LongAdder sendCount = new LongAdder();
    private final LongAdder sendMergeNanos = new LongAdder();
    private final LongAdder sendInsertNanos = new LongAdder();

    private final LongAdder ioTaskWorkerPoolWaitNanos = new LongAdder();
    private final LongAdder ioTaskWorkerPoolWaitCount = new LongAdder();

    private final LongAdder cpuTaskWorkerPoolWaitNanos = new LongAdder();
    private final LongAdder cpuTaskWorkerPoolWaitCount = new LongAdder();

    private final LongAdder writeTaskWorkerPoolWaitNanos = new LongAdder();
    private final LongAdder writeTaskWorkerPoolWaitCount = new LongAdder();

    private final LongAdder writeVsrCount = new LongAdder();
    private final LongAdder sentTaskCount = new LongAdder();
    private final LongAdder addedBufferVsrCount = new LongAdder();
    private final LongAdder removedBufferVsrCount = new LongAdder();

    private final LongAdder createdBufferCount = new LongAdder();
    private final LongAdder closedBufferCount = new LongAdder();

    private final LongAdder buffersSizeTotal = new LongAdder();
    private final LongAdder buffersSizeCount = new LongAdder();

    private final LongAccumulator maxBufferRowCount = new LongAccumulator(
            Math::max, 0L);

    public void recordBufferRowCount(long rowCount)
    {
        maxBufferRowCount.accumulate(rowCount);
    }

    public void recordSinglePartitionInsert(long rowCount)
    {
        partitionInserts.add(rowCount);
    }

    public void incFlushFull()
    {
        flushFullPartitionCount.increment();
    }

    public void incFlushFew()
    {
        flushFewPartitionsTogetherCount.increment();
    }

    public void recordFlushAllTime(long nanos)
    {
        flushAllNanos.add(nanos);
        flushAllCount.increment();
    }

    public void recordBuffersSize(long buffersSize)
    {
        buffersSizeTotal.add(buffersSize);
        buffersSizeCount.increment();
    }

    public void recordMaybeFlushSomeTime(long nanos)
    {
        maybeFlushSomeNanos.add(nanos);
        maybeFlushSomeCount.increment();
    }

    public void recordSendTime(long nanos)
    {
        sendNanos.add(nanos);
        sendCount.increment();
    }

    public void recordSendMergeTime(long nanos)
    {
        sendMergeNanos.add(nanos);
    }

    public void recordSendInsertTime(long nanos)
    {
        sendInsertNanos.add(nanos);
    }

    public void registerIoTaskWorkerPoolWaitTime(long nanos)
    {
        ioTaskWorkerPoolWaitNanos.add(nanos);
        ioTaskWorkerPoolWaitCount.increment();
    }

    public void registerCpuTaskWorkerPoolWaitTime(long nanos)
    {
        cpuTaskWorkerPoolWaitNanos.add(nanos);
        cpuTaskWorkerPoolWaitCount.increment();
    }

    public void recordWriteTaskWorkerPoolWaitTime(long nanos)
    {
        writeTaskWorkerPoolWaitNanos.add(nanos);
        writeTaskWorkerPoolWaitCount.increment();
    }

    public void recordWriteVsr()
    {
        writeVsrCount.increment();
    }

    public void registerSentTask()
    {
        sentTaskCount.increment();
    }

    public void incBufferVsrAdded()
    {
        addedBufferVsrCount.increment();
    }

    public void recordBufferVsrRemoved(int count)
    {
        removedBufferVsrCount.add(count);
    }

    public void incBufferCreated()
    {
        createdBufferCount.increment();
    }

    public void incBufferClosed()
    {
        closedBufferCount.increment();
    }

    @Managed
    public long getPartitionInsertsRowCountSum()
    {
        return partitionInserts.getSum();
    }

    @Managed
    public long getPartitionInsertsCount()
    {
        return partitionInserts.getCount();
    }

    @Managed
    public long getPartitionInsertsRowCountSumOfSquares()
    {
        return partitionInserts.getSumOfSquares();
    }

    @Managed
    public long getInsertBufferAllocatedMemory()
    {
        return insertBufferAllocatedMemory.get();
    }

    public void setInsertBufferAllocatedMemory(long allocatedMemoryBytes)
    {
        insertBufferAllocatedMemory.set(allocatedMemoryBytes);
    }

    @Managed
    public long getMaxPartitionInsertRowCount()
    {
        return partitionInserts.getMax();
    }

    @Managed
    public long getMinPartitionInsertRowCount()
    {
        return partitionInserts.getMin();
    }

    @Managed
    public long getFlushFewPartitionsTogetherCount()
    {
        return flushFewPartitionsTogetherCount.sum();
    }

    @Managed
    public long getFlushFullPartitionCount()
    {
        return flushFullPartitionCount.sum();
    }

    @Managed
    public long getFlushAllNanos()
    {
        return flushAllNanos.sum();
    }

    @Managed
    public long getFlushAllCount()
    {
        return flushAllCount.sum();
    }

    @Managed
    public long getMaybeFlushSomeNanos()
    {
        return maybeFlushSomeNanos.sum();
    }

    @Managed
    public long getMaybeFlushSomeCount()
    {
        return maybeFlushSomeCount.sum();
    }

    @Managed
    public long getSendNanos()
    {
        return sendNanos.sum();
    }

    @Managed
    public long getSendCount()
    {
        return sendCount.sum();
    }

    @Managed
    public long getSendMergeNanos()
    {
        return sendMergeNanos.sum();
    }

    @Managed
    public long getSendInsertNanos()
    {
        return sendInsertNanos.sum();
    }

    @Managed
    public long getIoTaskWorkerPoolWaitNanos()
    {
        return ioTaskWorkerPoolWaitNanos.sum();
    }

    @Managed
    public long getIoTaskWorkerPoolWaitCount()
    {
        return ioTaskWorkerPoolWaitCount.sum();
    }

    @Managed
    public long getCpuTaskWorkerPoolWaitNanos()
    {
        return cpuTaskWorkerPoolWaitNanos.sum();
    }

    @Managed
    public long getCpuTaskWorkerPoolWaitCount()
    {
        return cpuTaskWorkerPoolWaitCount.sum();
    }

    @Managed
    public long getWriteTaskWorkerPoolWaitNanos()
    {
        return writeTaskWorkerPoolWaitNanos.sum();
    }

    @Managed
    public long getWriteTaskWorkerPoolWaitCount()
    {
        return writeTaskWorkerPoolWaitCount.sum();
    }

    @Managed
    public long getWriteVsrCount()
    {
        return writeVsrCount.sum();
    }

    @Managed
    public long getSentTaskCount()
    {
        return sentTaskCount.sum();
    }

    @Managed
    public long getAddedBufferVsrCount()
    {
        return addedBufferVsrCount.sum();
    }

    @Managed
    public long getRemovedBufferVsrCount()
    {
        return removedBufferVsrCount.sum();
    }

    @Managed
    public long getCreatedBufferCount()
    {
        return createdBufferCount.sum();
    }

    @Managed
    public long getClosedBufferCount()
    {
        return closedBufferCount.sum();
    }

    @Managed
    public long getOpenBufferVsrCount()
    {
        return addedBufferVsrCount.sum() - removedBufferVsrCount.sum();
    }

    @Managed
    public long getBufferCount()
    {
        return createdBufferCount.sum() - closedBufferCount.sum();
    }

    @Managed
    public long getBuffersSizeTotal()
    {
        return buffersSizeTotal.sum();
    }

    @Managed
    public long getBuffersSizeCount()
    {
        return buffersSizeCount.sum();
    }

    @Managed
    public long getMaxBufferRowCount()
    {
        return maxBufferRowCount.get();
    }

    @Override
    public Map<String, Long> asMap()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("partitionInsertsRowCountSum",
                getPartitionInsertsRowCountSum());
        map.put("partitionInsertsCount", getPartitionInsertsCount());
        map.put("partitionInsertsRowCountSumOfSquares",
                getPartitionInsertsRowCountSumOfSquares());
        map.put("insertBufferAllocatedMemory",
                getInsertBufferAllocatedMemory());
        map.put("maxPartitionInsertRowCount", getMaxPartitionInsertRowCount());
        map.put("minPartitionInsertRowCount", getMinPartitionInsertRowCount());
        map.put("flushFewPartitionsTogetherCount",
                getFlushFewPartitionsTogetherCount());
        map.put("flushFullPartitionCount", getFlushFullPartitionCount());
        map.put("flushAllNanos", getFlushAllNanos());
        map.put("flushAllCount", getFlushAllCount());
        map.put("maybeFlushSomeNanos", getMaybeFlushSomeNanos());
        map.put("maybeFlushSomeCount", getMaybeFlushSomeCount());
        map.put("sendNanos", getSendNanos());
        map.put("sendCount", getSendCount());
        map.put("sendMergeNanos", getSendMergeNanos());
        map.put("sendInsertNanos", getSendInsertNanos());
        map.put("ioTaskWorkerPoolWaitNanos", getIoTaskWorkerPoolWaitNanos());
        map.put("ioTaskWorkerPoolWaitCount", getIoTaskWorkerPoolWaitCount());
        map.put("cpuTaskWorkerPoolWaitNanos", getCpuTaskWorkerPoolWaitNanos());
        map.put("cpuTaskWorkerPoolWaitCount", getCpuTaskWorkerPoolWaitCount());
        map.put("writeTaskWorkerPoolWaitNanos",
                getWriteTaskWorkerPoolWaitNanos());
        map.put("writeTaskWorkerPoolWaitCount",
                getWriteTaskWorkerPoolWaitCount());
        map.put("writeVsrCount", getWriteVsrCount());
        map.put("sentTaskCount", getSentTaskCount());
        map.put("addedBufferVsrCount", getAddedBufferVsrCount());
        map.put("removedBufferVsrCount", getRemovedBufferVsrCount());
        map.put("createdBufferCount", getCreatedBufferCount());
        map.put("closedBufferCount", getClosedBufferCount());
        map.put("openBufferVsrCount", getOpenBufferVsrCount());
        map.put("bufferCount", getBufferCount());

        return map;
    }

    @Override
    public Map<String, Long> diffMetrics()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("partitionInsertsRowCountSum",
                getPartitionInsertsRowCountSum());
        map.put("partitionInsertsCount", getPartitionInsertsCount());
        map.put("flushFewPartitionsTogetherCount",
                getFlushFewPartitionsTogetherCount());
        map.put("flushFullPartitionCount", getFlushFullPartitionCount());
        map.put("flushAllNanos", getFlushAllNanos());
        map.put("flushAllCount", getFlushAllCount());
        map.put("maybeFlushSomeNanos", getMaybeFlushSomeNanos());
        map.put("maybeFlushSomeCount", getMaybeFlushSomeCount());
        map.put("sendNanos", getSendNanos());
        map.put("sendCount", getSendCount());
        map.put("sendMergeNanos", getSendMergeNanos());
        map.put("sendInsertNanos", getSendInsertNanos());
        map.put("ioTaskWorkerPoolWaitNanos", getIoTaskWorkerPoolWaitNanos());
        map.put("ioTaskWorkerPoolWaitCount", getIoTaskWorkerPoolWaitCount());
        map.put("cpuTaskWorkerPoolWaitNanos", getCpuTaskWorkerPoolWaitNanos());
        map.put("cpuTaskWorkerPoolWaitCount", getCpuTaskWorkerPoolWaitCount());
        map.put("writeTaskWorkerPoolWaitNanos",
                getWriteTaskWorkerPoolWaitNanos());
        map.put("writeTaskWorkerPoolWaitCount",
                getWriteTaskWorkerPoolWaitCount());
        map.put("writeVsrCount", getWriteVsrCount());
        map.put("sentTaskCount", getSentTaskCount());
        map.put("addedBufferVsrCount", getAddedBufferVsrCount());
        map.put("removedBufferVsrCount", getRemovedBufferVsrCount());
        map.put("createdBufferCount", getCreatedBufferCount());
        map.put("closedBufferCount", getClosedBufferCount());
        return map;
    }

    @Override
    public Map<String, Long> stateMetrics()
    {
        Map<String, Long> map = new HashMap<>();
        map.put("insertBufferAllocatedMemory",
                getInsertBufferAllocatedMemory());
        map.put("openBufferVsrCount", getOpenBufferVsrCount());
        map.put("bufferCount", getBufferCount());

        return map;
    }

    @Override
    public void merge(BufferedInsertMetrics other)
    {
        if (other == null || other == this) {
            return;
        }
        this.partitionInserts.merge(other.partitionInserts);
        this.insertBufferAllocatedMemory.addAndGet(
                other.insertBufferAllocatedMemory.get());

        this.flushFullPartitionCount.add(other.flushFullPartitionCount.sum());
        this.flushFewPartitionsTogetherCount.add(
                other.flushFewPartitionsTogetherCount.sum());

        this.flushAllNanos.add(other.flushAllNanos.sum());
        this.flushAllCount.add(other.flushAllCount.sum());
        this.maybeFlushSomeNanos.add(other.maybeFlushSomeNanos.sum());
        this.maybeFlushSomeCount.add(other.maybeFlushSomeCount.sum());

        this.sendNanos.add(other.sendNanos.sum());
        this.sendCount.add(other.sendCount.sum());
        this.sendMergeNanos.add(other.sendMergeNanos.sum());
        this.sendInsertNanos.add(other.sendInsertNanos.sum());

        this.ioTaskWorkerPoolWaitNanos.add(
                other.ioTaskWorkerPoolWaitNanos.sum());
        this.ioTaskWorkerPoolWaitCount.add(
                other.ioTaskWorkerPoolWaitCount.sum());
        this.cpuTaskWorkerPoolWaitNanos.add(
                other.cpuTaskWorkerPoolWaitNanos.sum());
        this.cpuTaskWorkerPoolWaitCount.add(
                other.cpuTaskWorkerPoolWaitCount.sum());
        this.writeTaskWorkerPoolWaitNanos.add(
                other.writeTaskWorkerPoolWaitNanos.sum());
        this.writeTaskWorkerPoolWaitCount.add(
                other.writeTaskWorkerPoolWaitCount.sum());

        this.writeVsrCount.add(other.writeVsrCount.sum());
        this.sentTaskCount.add(other.sentTaskCount.sum());
        this.addedBufferVsrCount.add(other.addedBufferVsrCount.sum());
        this.removedBufferVsrCount.add(other.removedBufferVsrCount.sum());
        this.createdBufferCount.add(other.createdBufferCount.sum());
        this.closedBufferCount.add(other.closedBufferCount.sum());
    }

    @Override
    public BufferedInsertMetrics copy()
    {
        BufferedInsertMetrics clone = new BufferedInsertMetrics();
        clone.partitionInserts.merge(this.partitionInserts);
        clone.insertBufferAllocatedMemory.set(
                this.insertBufferAllocatedMemory.get());
        clone.flushFullPartitionCount.add(this.flushFullPartitionCount.sum());
        clone.flushFewPartitionsTogetherCount.add(
                this.flushFewPartitionsTogetherCount.sum());

        clone.flushAllNanos.add(this.flushAllNanos.sum());
        clone.flushAllCount.add(this.flushAllCount.sum());
        clone.maybeFlushSomeNanos.add(this.maybeFlushSomeNanos.sum());
        clone.maybeFlushSomeCount.add(this.maybeFlushSomeCount.sum());

        clone.sendNanos.add(this.sendNanos.sum());
        clone.sendCount.add(this.sendCount.sum());
        clone.sendMergeNanos.add(this.sendMergeNanos.sum());
        clone.sendInsertNanos.add(this.sendInsertNanos.sum());

        clone.ioTaskWorkerPoolWaitNanos.add(
                this.ioTaskWorkerPoolWaitNanos.sum());
        clone.ioTaskWorkerPoolWaitCount.add(
                this.ioTaskWorkerPoolWaitCount.sum());
        clone.cpuTaskWorkerPoolWaitNanos.add(
                this.cpuTaskWorkerPoolWaitNanos.sum());
        clone.cpuTaskWorkerPoolWaitCount.add(
                this.cpuTaskWorkerPoolWaitCount.sum());
        clone.writeTaskWorkerPoolWaitNanos.add(
                this.writeTaskWorkerPoolWaitNanos.sum());
        clone.writeTaskWorkerPoolWaitCount.add(
                this.writeTaskWorkerPoolWaitCount.sum());

        clone.writeVsrCount.add(this.writeVsrCount.sum());
        clone.sentTaskCount.add(this.sentTaskCount.sum());
        clone.addedBufferVsrCount.add(this.addedBufferVsrCount.sum());
        clone.removedBufferVsrCount.add(this.removedBufferVsrCount.sum());
        clone.createdBufferCount.add(this.createdBufferCount.sum());
        clone.closedBufferCount.add(this.closedBufferCount.sum());

        return clone;
    }
}
