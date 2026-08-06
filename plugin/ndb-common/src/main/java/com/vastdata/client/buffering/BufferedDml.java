/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.buffering;

import com.vastdata.client.error.ErrorType;
import com.vastdata.client.error.VastExceptionFactory;
import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.client.metrics.BufferedInsertMetrics;
import com.vastdata.client.metrics.InsertedRowsStats;
import com.vastdata.client.metrics.TimeMeasure;
import io.airlift.log.Logger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.String.format;

public class BufferedDml
{
    private static final Logger LOG = Logger.get(BufferedDml.class);

    private final Config config;

    private final BufferAllocator allocator;
    private final BufferAllocator insertBuffersAllocator;
    private final BufferMap bufferMap;
    private final BufferedInsertMetrics metrics;
    private final BufferedTaskFactory taskFactory;

    private final ExecutorService cpuExecutor;

    private final AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();

    private final AtomicBoolean closed;

    private final InsertedRowsStats insertedRowsStats;

    private final Semaphore jobSemaphore;
    private final Semaphore writeSemaphore;
    private final Set<CompletableFuture<?>> activeTasks = ConcurrentHashMap.newKeySet();
    private final Consumer<Throwable> reportException = (e) -> {
        // Capture the first exception that occurs
        if (backgroundFailure.compareAndSet(null, e)) {
            LOG.error(e, "Background insert task failed");
        }
    };

    public BufferedDml(Config config, BufferAllocator allocator,
            BufferAllocator insertBuffersAllocator,
            BufferedInsertMetrics globalVastBufferedInsertMetrics,
            BufferedTaskFactory taskFactory,
            InsertedRowsStats insertedRowsStats, ExecutorService vastIoExecutor,
            ExecutorService vastCpuExecutor)
    {
        this.config = config;

        this.allocator = allocator;
        this.insertBuffersAllocator = insertBuffersAllocator;

        this.closed = new AtomicBoolean(false);
        this.taskFactory = taskFactory;
        this.bufferMap = new BufferMap(globalVastBufferedInsertMetrics,
                allocator, config);
        this.metrics = globalVastBufferedInsertMetrics;
        this.insertedRowsStats = insertedRowsStats;
        this.cpuExecutor = vastCpuExecutor;

        this.jobSemaphore = new Semaphore(config.getMaxJobPermits());
        this.writeSemaphore = new Semaphore(config.getMaxWritePermits());
    }

    /**
     * Approximates the in-memory size of a VectorSchemaRoot.
     * <p>
     * This method calculates the total size of all underlying buffers for all
     * vectors in the root. It serves as a good proxy for the final serialized
     * size, which is useful for determining when to flush the buffers.
     *
     * @param root The VectorSchemaRoot to measure.
     * @return The estimated size in bytes as a long.
     */
    public static long calcApproximateSizeInBytes(VectorSchemaRoot root)
    {
        return root
                .getFieldVectors()
                .stream()
                .mapToLong(FieldVector::getBufferSize)
                .sum();
    }

    public CompletableFuture<?> write(Map<Long, VsrAppender> bufferIdToAppender)
            throws VastRuntimeException
    {
        if (closed.get()) {
            throw new RuntimeException(
                    "Should be unreachable code: BufferedInserter is already closed");
        }

        metrics.recordWriteVsr();
        checkFailure();

        AtomicReference<CompletableFuture<?>> flushedFull = new AtomicReference<>(
                null);

        try {
            executeTask(() -> CompletableFuture.runAsync(() -> {
                List<Buffer> fullBuffers = new ArrayList<>();

                for (Map.Entry<Long, VsrAppender> bufferIdAndAppender : bufferIdToAppender.entrySet()) {
                    Long bufferId = bufferIdAndAppender.getKey();
                    try (VsrAppender appender = bufferIdAndAppender.getValue()) {
                        Optional<Buffer> maybeFullBuffer = bufferMap.putAndRemoveIfFull(
                                bufferId, appender);
                        maybeFullBuffer.ifPresent(fullBuffers::add);
                        insertedRowsStats.addRowsReceivedByBufferedInserter(
                                appender.getRowCount());
                    }
                    ArrayList<CompletableFuture<?>> sendFutures = new ArrayList<>();
                    for (Buffer buffer : fullBuffers) {
                        LOG.debug("Got a full buffer, sending it. rowCount: %s",
                                buffer.getRowCount());
                        metrics.incFlushFull();
                        metrics.recordSinglePartitionInsert(
                                buffer.getRowCount());
                        sendFutures.add(send(List.of(buffer)));
                    }
                    flushedFull.set(CompletableFuture.allOf(
                            sendFutures.toArray(new CompletableFuture[0])));
                }
            }, cpuExecutor), writeSemaphore);
        }
        catch (Throwable e) {
            reportException.accept(new RuntimeException(e.getMessage(), e));
            return CompletableFuture.failedFuture(e);
        }

        if (isMemoryUsageOverSoftLimit()) {
            if (flushedFull.get() != null) {
                return flushedFull.get();
            }
            Optional<CompletableFuture<?>> flushFuture = maybeFlushSome();
            if (flushFuture.isPresent()) {
                return flushFuture.get();
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<?> executeTask(
            Supplier<CompletableFuture<?>> futureSupplier,
            Semaphore taskSemaphore)
            throws VastRuntimeException
    {
        taskSemaphore.acquireUninterruptibly();
        CompletableFuture<?> future;

        try {
            future = futureSupplier.get();
        }
        catch (Throwable e) {
            taskSemaphore.release();
            throw e;
        }

        activeTasks.add(future);

        future.whenComplete((res, err) -> {
            activeTasks.remove(future);

            if (err != null && !(err instanceof CancellationException)) {
                reportException.accept(err);
            }

            taskSemaphore.release();
        });

        return future;
    }

    private Optional<CompletableFuture<?>> maybeFlushSome()
            throws VastRuntimeException
    {
        LOG.debug("try to flush some buffers");
        checkFailure();
        LOG.debug("maybeFlushSome start");
        TimeMeasure timeMeasure = TimeMeasure.createAndStart();
        try {
            List<Buffer> buffersToSend = bufferMap.extractLargestBuffers(
                    config.getMaxRequestBodySize());
            if (buffersToSend.isEmpty()) {
                LOG.debug("maybeFlushSome end (empty)");
                return Optional.empty();
            }

            for (Buffer buffer : buffersToSend) {
                metrics.recordSinglePartitionInsert(buffer.getRowCount());
            }

            metrics.incFlushFew();
            return Optional.of(send(buffersToSend));
        }
        finally {
            timeMeasure.end(metrics::recordMaybeFlushSomeTime);
            LOG.debug("maybeFlushSome end");
        }
    }

    private void waitForAllWritePermits()
    {
        LOG.debug("Waiting for write tasks to complete...");
        try {
            writeSemaphore.acquire(this.config.getMaxWritePermits());
            writeSemaphore.release(this.config.getMaxWritePermits());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Failed waiting for all write permits", e);
            throw new VastRuntimeException(
                    "Failed to wait for all outstanding append jobs", e,
                    ErrorType.CLIENT);
        }
    }

    private void waitForAllJobPermits()
    {
        LOG.debug("Waiting for job tasks to complete...");
        try {
            jobSemaphore.acquire(this.config.getMaxJobPermits());
            jobSemaphore.release(this.config.getMaxJobPermits());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Failed waiting for all job permits", e);
            throw new VastRuntimeException(
                    "Failed to wait for all outstanding tasks", e,
                    ErrorType.CLIENT);
        }
    }

    public void flushAllAndFinish()
            throws VastRuntimeException
    {
        checkFailure();

        closed.set(true);

        TimeMeasure timeMeasure = TimeMeasure.createAndStart();

        waitForAllWritePermits();

        flushFullBuffers();

        while (true) {
            List<Buffer> buffersToFlush = bufferMap.extractLargestBuffers(
                    config.getMaxRequestBodySize());

            if (buffersToFlush.isEmpty()) {
                break; // No more buffers to process
            }

            send(buffersToFlush);
        }

        waitForAllJobPermits();

        timeMeasure.end(metrics::recordFlushAllTime);

        checkFailure();
    }

    private CompletableFuture<?> send(List<Buffer> buffers)
            throws VastRuntimeException
    {
        try {
            int nRows = buffers.stream().mapToInt(Buffer::getRowCount).sum();
            insertedRowsStats.addRowsToSend(nRows);
            metrics.registerSentTask();

            return executeTask(
                    () -> taskFactory.executeAsync(buffers, metrics, allocator),
                    jobSemaphore);
        }
        catch (Throwable e) {
            buffers.forEach(Buffer::close);
            VastRuntimeException exc = new VastRuntimeException(
                    "Failed to send buffers", e, ErrorType.CLIENT);
            reportException.accept(exc);
            throw exc;
        }
    }

    private void checkFailure()
            throws VastRuntimeException
    {
        Throwable t = backgroundFailure.get();
        if (t != null) {
            throw VastExceptionFactory.toRuntime(
                    format("BufferedDml: Buffered %s operation failed: %s",
                            taskFactory.getTaskName(), t), t);
        }
    }

    private void flushFullBuffers()
            throws VastRuntimeException
    {
        List<Buffer> fullBuffers = bufferMap.extractFullBuffers();
        if (fullBuffers.isEmpty()) {
            return;
        }
        try {
            for (Buffer buffer : fullBuffers) {
                metrics.incFlushFull();
                metrics.recordSinglePartitionInsert(buffer.getRowCount());
                send(List.of(buffer));
            }
        }
        catch (Throwable e) {
            fullBuffers.forEach(Buffer::close);
            throw VastExceptionFactory.toRuntime("Failed to send buffers", e);
        }
    }

    private synchronized boolean isMemoryUsageOverSoftLimit()
    {
        long allocatedMemory = insertBuffersAllocator.getAllocatedMemory();
        metrics.setInsertBufferAllocatedMemory(allocatedMemory);

        boolean overLimit = allocatedMemory > config.getSoftLimitInBytes();
        if (overLimit) {
            LOG.debug(
                    "Memory Usage is over soft limit. current: %d, threshold: %d",
                    allocatedMemory, config.getSoftLimitInBytes());
        }
        return overLimit;
    }

    public void abort()
    {
        LOG.debug("Aborting Buffered Insert");
        reportException.accept(new Exception("Query Aborted"));
        closed.set(true);

        waitForAllWritePermits();
        waitForAllJobPermits();

        close();
    }

    public void close()
    {
        metrics.setInsertBufferAllocatedMemory(
                insertBuffersAllocator.getAllocatedMemory());
        bufferMap.close();
    }

    public static class Config
    {
        private final long maxRequestBodySize;

        private final long softLimitInBytes;

        private final int bufferOpenVsrTargetRowCount;
        private final int bufferOpenVsrRowCountPreallocation;
        private final int targetRowCountPerPartitionFlush;

        private final int maxWritePermits;
        private final int maxJobPermits;

        public Config(int bufferOpenVsrTargetRowCount, long maxRequestBodySize,
                int bufferOpenVsrRowCountPreallocation, long softLimitInBytes,
                int targetRowCountPerPartitionFlush, int maxWritePermits,
                int maxJobPermits)
        {
            this.bufferOpenVsrTargetRowCount = bufferOpenVsrTargetRowCount;
            this.maxRequestBodySize = maxRequestBodySize;
            this.bufferOpenVsrRowCountPreallocation = bufferOpenVsrRowCountPreallocation;
            this.softLimitInBytes = softLimitInBytes;
            this.targetRowCountPerPartitionFlush = targetRowCountPerPartitionFlush;
            this.maxWritePermits = maxWritePermits;
            this.maxJobPermits = maxJobPermits;
        }

        public int getBufferOpenVsrTargetRowCount()
        {
            return bufferOpenVsrTargetRowCount;
        }

        public long getMaxRequestBodySize()
        {
            return maxRequestBodySize;
        }

        public int getBufferOpenVsrRowCountPreallocation()
        {
            return bufferOpenVsrRowCountPreallocation;
        }

        public long getSoftLimitInBytes()
        {
            return softLimitInBytes;
        }

        public long getTargetRowCountPerPartitionFlush()
        {
            return targetRowCountPerPartitionFlush;
        }

        public int getMaxWritePermits()
        {
            return maxWritePermits;
        }

        public int getMaxJobPermits()
        {
            return maxJobPermits;
        }

        @Override
        public String toString()
        {
            return ReflectionToStringBuilder.toString(this);
        }
    }
}
