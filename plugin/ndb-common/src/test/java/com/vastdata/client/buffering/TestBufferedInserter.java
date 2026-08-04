/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.buffering;

import com.vastdata.client.VastConfig;
import com.vastdata.client.metrics.BufferedInsertMetrics;
import com.vastdata.client.metrics.InsertedRowsStats;
import com.vastdata.client.rowid.RowIdListSchemaFactory;
import com.vastdata.client.rowid.TableType;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBufferedInserter
{
    private static final Logger LOG = Logger.get(TestBufferedInserter.class);

    private BufferedDml.Config config;
    private BufferedTaskFactory taskFactory;
    private Schema schema;
    private ExecutorService executor;

    @BeforeClass
    public void beforeClass()
    {
        Logging.initialize().setLevel("com.vastdata", Level.INFO);
        System.setProperty("arrow.memory.debug.allocator", "true");
        executor = new ThreadPoolExecutor(1, 4, 15, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
    }

    @AfterClass
    public void afterClass()
    {
        executor.shutdown();
    }

    @BeforeMethod
    public void setup()
    {
        VastConfig vastConfig = new VastConfig();
        this.config = buildBufferedConfig(vastConfig);
        this.taskFactory = new TestBufferedTaskFactory();
        this.schema = RowIdListSchemaFactory.get(TableType.SORTED);
    }

    private BufferedDml.Config buildBufferedConfig(VastConfig vastConfig)
    {
        return new BufferedDml.Config(
                vastConfig.getBufferingBufferOpenVsrTargetRowCount(),
                vastConfig.getMaxRequestBodySize(), 10,
                vastConfig.getBufferingBufferSizeSoftLimit().toBytes(), 10,
                vastConfig.getBufferedInserterMaxWritePermits(),
                vastConfig.getBufferedInserterMaxJobPermits());
    }

    @Test
    public void testInsertMoreThanMaxPerPartitionShouldSend()
    {
        try (RootAllocator allocator = new RootAllocator(); BufferAllocator insertBufferAllocator = allocator.newChildAllocator(
                "insertBuffer", 0, Long.MAX_VALUE)) {
            int totalRpcCalls = 0;
            BufferedInsertMetrics metric = new BufferedInsertMetrics();
            BufferedDml bufferedInserter = new BufferedDml(config, allocator,
                    insertBufferAllocator, metric, taskFactory,
                    new InsertedRowsStats(), executor, executor);
            TestVsrAppender testVsrAppender = getVsrAppender(11, allocator);
            writeAndWait(bufferedInserter, testVsrAppender, metric, allocator);
            totalRpcCalls++;
            waitForExecution(metric, totalRpcCalls, allocator);
            bufferedInserter.close();
        }
    }

    @Test
    public void testInsertAccumulatePartition()
    {
        try (RootAllocator allocator = new RootAllocator(); BufferAllocator insertBufferAllocator = allocator.newChildAllocator(
                "insertBuffer", 0, Long.MAX_VALUE)) {
            int totalRpcCalls = 0;
            VastConfig vastConfig = new VastConfig();
            vastConfig.setInsertBufferTargetRowCountPerPartitionFlush(8);
            this.config = buildBufferedConfig(vastConfig);
            BufferedInsertMetrics metric = new BufferedInsertMetrics();
            BufferedDml bufferedInserter = new BufferedDml(config, allocator,
                    insertBufferAllocator, metric, taskFactory,
                    new InsertedRowsStats(), executor, executor);
            TestVsrAppender testVsrAppender = getVsrAppender(5, allocator);
            writeAndWait(bufferedInserter, testVsrAppender, metric, allocator);
            LOG.info("adding 5 should not trigger flush - %d",
                    allocator.getAllocatedMemory());
            testVsrAppender.close();
            waitForExecution(metric, totalRpcCalls, allocator);
            LOG.info(
                    "maybeFlush with no new data should not trigger flush - %d",
                    allocator.getAllocatedMemory());
            assertThat(metric.getSendCount()).isEqualTo(totalRpcCalls);
            testVsrAppender = getVsrAppender(5, allocator);
            writeAndWait(bufferedInserter, testVsrAppender, metric, allocator);
            LOG.info("adding 5 SHOULD trigger flush- %d",
                    allocator.getAllocatedMemory());
            totalRpcCalls++;
            waitForExecution(metric, totalRpcCalls, allocator);
            LOG.info("adding more should not trigger another flush");
            testVsrAppender = getVsrAppender(5, allocator);
            writeAndWait(bufferedInserter, testVsrAppender, metric, allocator);
            LOG.info("adding 5 should not trigger flush");
            waitForExecution(metric, totalRpcCalls, allocator);
            bufferedInserter.flushAllAndFinish();
            totalRpcCalls++;
            waitForExecution(metric, totalRpcCalls, allocator);
            bufferedInserter.close();
        }
    }

    private void writeAndWait(BufferedDml bufferedInserter,
            VsrAppender vsrAppender, BufferedInsertMetrics metric,
            BufferAllocator allocator)
    {
        long expectedValue = metric.getWriteVsrCount() + 1;
        Map<Long, VsrAppender> bufferIdToAppender = Map.of(1L, vsrAppender);
        bufferedInserter.write(bufferIdToAppender);
        RetryPolicy<Object> retryPolicy = new RetryPolicy<>();
        retryPolicy.handle(AssertionError.class);
        retryPolicy.withMaxRetries(100);
        retryPolicy.withDelay(Duration.ofMillis(100));
        Failsafe.with(retryPolicy).run(() -> {
            if (metric.getWriteVsrCount() != expectedValue) {
                throw new AssertionError("Value still matches expected value");
            }
            else {
                LOG.info("write buffer succeeded- %d",
                        allocator.getAllocatedMemory());
            }
        });
    }

    private void waitForExecution(BufferedInsertMetrics metric,
            long expectedValue, BufferAllocator allocator)
    {
        RetryPolicy<Object> retryPolicy = new RetryPolicy<>();
        retryPolicy.handle(AssertionError.class);
        retryPolicy.withMaxRetries(100);
        retryPolicy.withDelay(Duration.ofMillis(100));
        Failsafe.with(retryPolicy).run(() -> {
            if (metric.getSendCount() != expectedValue) {
                throw new AssertionError(String.format(
                        "Value does not match expected value got:%s, expected:%s",
                        metric.getSendCount(), expectedValue));
            }
            else {
                LOG.info("execution succeeded %d",
                        allocator.getAllocatedMemory());
            }
        });
    }

    private TestVsrAppender getVsrAppender(int rowCount,
            BufferAllocator allocator)
    {
        VectorSchemaRoot v = VectorSchemaRoot.create(schema, allocator);
        DecimalVector rowIdsVector = new DecimalVector("$row_id", allocator, 38,
                0);
        rowIdsVector.allocateNew();
        IntStream.range(0, rowCount).forEach(i -> rowIdsVector.set(i, 1));
        VectorSchemaRoot ret = v.addVector(0, rowIdsVector);
        v.close();
        ret.setRowCount(rowCount);
        return new TestVsrAppender(ret, allocator);
    }

    private static class TestBufferedTaskFactory
            implements BufferedTaskFactory
    {
        public CompletableFuture<Void> executeAsync(List<Buffer> buffers,
                BufferedInsertMetrics metrics, BufferAllocator allocator)
        {
            TestBufferedTask task = new TestBufferedTask(buffers, metrics,
                    allocator);
            return CompletableFuture.runAsync(task);
        }

        @Override
        public String getTaskName()
        {
            return "TestBufferedTask";
        }
    }

    private static class TestBufferedTask
            implements Runnable
    {
        private final List<Buffer> buffers;
        private final BufferedInsertMetrics metrics;
        private final BufferAllocator allocator;

        public TestBufferedTask(List<Buffer> buffers,
                BufferedInsertMetrics metrics, BufferAllocator allocator)
        {
            this.buffers = buffers;
            this.metrics = metrics;
            this.allocator = allocator;
        }

        public void run()
        {
            LOG.info("Starting test buffered inserter - %d",
                    allocator.getAllocatedMemory());
            buffers.forEach(Buffer::close);
            LOG.info("finish test buffered inserter - %d",
                    allocator.getAllocatedMemory());
            metrics.recordSendTime(1);
        }
    }

    private static class TestVsrAppender
            implements VsrAppender
    {
        private final VectorSchemaRoot vsr;
        private final BufferAllocator allocator;

        public TestVsrAppender(VectorSchemaRoot vectorSchemaRoot,
                BufferAllocator allocator)
        {
            this.vsr = vectorSchemaRoot;
            this.allocator = allocator;
        }

        @Override
        public void append(VectorSchemaRoot root)
        {
            VectorSchemaRootAppender.append(false, root, vsr);
            LOG.info("after append - %d", allocator.getAllocatedMemory());
        }

        @Override
        public Schema getSchema()
        {
            return RowIdListSchemaFactory.get(TableType.SORTED);
        }

        @Override
        public void close()
        {
            LOG.info("before closing vsr %d - %d", System.identityHashCode(vsr),
                    allocator.getAllocatedMemory());
            vsr.close();
            LOG.info("after closing vsr %d - %d", System.identityHashCode(vsr),
                    allocator.getAllocatedMemory());
        }

        @Override
        public Integer getRowCount()
        {
            return vsr.getRowCount();
        }
    }
}
