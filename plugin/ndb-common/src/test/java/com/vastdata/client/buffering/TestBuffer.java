/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.buffering;

import com.vastdata.client.VastConfig;
import com.vastdata.client.buffering.BufferedDml.Config;
import com.vastdata.client.metrics.BufferedInsertMetrics;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestBuffer
{
    @Mock private BufferedInsertMetrics metrics;
    @Mock private VsrAppender vsrAppender;

    private Config config;

    private BufferAllocator allocator;
    private Schema schema;
    private Buffer buffer;

    @BeforeMethod
    public void setup()
    {
        MockitoAnnotations.openMocks(this);
        clearInvocations(metrics);
        allocator = new RootAllocator(Long.MAX_VALUE);

        // Define a simple schema with one Int column
        schema = new Schema(Collections.singletonList(
                Field.nullable("col1", new ArrowType.Int(32, true))));

        when(vsrAppender.getSchema()).thenReturn(schema);

        VastConfig vastConfig = new VastConfig();
        this.config = new Config(10, vastConfig.getMaxRequestBodySize(),
                vastConfig.getBufferingBufferOpenVsrRowCountPreallocation(),
                vastConfig.getBufferingBufferSizeSoftLimit().toBytes(),
                vastConfig.getInsertBufferTargetRowCountPerPartitionFlush(),
                vastConfig.getBufferedInserterMaxWritePermits(),
                vastConfig.getBufferedInserterMaxJobPermits());

        buffer = new Buffer(metrics, allocator, config);
    }

    @AfterMethod
    public void tearDown()
    {
        if (buffer != null) {
            buffer.close();
        }
        allocator.close();
    }

    @Test
    public void testInitialization()
    {
        assertEquals(buffer.getRowCount(), 0);
        assertEquals(buffer.getVsrCount(), 0);
        verify(metrics).incBufferCreated();
    }

    @Test
    public void testAddRowsPartialFill()
    {
        clearInvocations(metrics);
        int rowsToAdd = 4;
        setupAppenderMock(rowsToAdd);

        buffer.add(vsrAppender);

        assertEquals(buffer.getRowCount(), rowsToAdd);
        assertEquals(buffer.getVsrCount(), 1); // 1 open VSR

        verify(metrics).incBufferVsrAdded();
        vsrAppender.close();
    }

    @Test
    public void testAddRowsExactFillClosesVsr()
    {
        clearInvocations(metrics);
        int rowsToAdd = config.getBufferOpenVsrTargetRowCount();
        setupAppenderMock(rowsToAdd);

        buffer.add(vsrAppender);

        assertEquals(buffer.getRowCount(), rowsToAdd);
        assertEquals(buffer.getVsrCount(), 1); // Moved to closed list
    }

    @Test
    public void testAddRowsMultipleBatchesOverflow()
    {
        clearInvocations(metrics);
        // 1. Add 6 rows (Buffer: 6/10)
        setupAppenderMock(6);
        buffer.add(vsrAppender);
        assertEquals(buffer.getRowCount(), 6);
        assertEquals(buffer.getVsrCount(), 1); // Open

        // 2. Add 5 rows (Buffer: 11/10) -> Should close the VSR
        // Note: The Buffer logic appends to the *existing* open VSR.
        // If the resulting size >= target, it closes it.
        // So the single VSR will hold 11 rows.
        setupAppenderMock(5);
        buffer.add(vsrAppender);

        assertEquals(buffer.getRowCount(), 11);
        assertEquals(buffer.getVsrCount(), 1); // It is now in the closed list

        // 3. Add 1 row -> New Open VSR created
        setupAppenderMock(1);
        buffer.add(vsrAppender);

        assertEquals(buffer.getRowCount(), 12);
        assertEquals(buffer.getVsrCount(), 2); // 1 closed + 1 open
    }

    @Test
    public void testGetVsrsAndClose()
    {
        clearInvocations(metrics);
        // Fill one buffer to completion
        setupAppenderMock(config.getBufferOpenVsrTargetRowCount());
        buffer.add(vsrAppender);

        // Fill another partially
        setupAppenderMock(2);
        buffer.add(vsrAppender);

        assertEquals(buffer.getVsrCount(), 2);

        List<VectorSchemaRoot> result = buffer.moveVsrs(allocator);

        assertEquals(result.size(), 2);
        assertEquals(result.get(0).getRowCount(),
                config.getBufferOpenVsrTargetRowCount());
        assertEquals(result.get(1).getRowCount(), 2);

        buffer.close();
        assertEquals(buffer.getVsrCount(), 0);
        verify(metrics).recordBufferVsrRemoved(2);

        // Clean up the returned roots manually since the buffer relinquished ownership
        result.forEach(VectorSchemaRoot::close);
        buffer.close();
    }

    @Test
    public void testApproximateSerializedBytes()
    {
        clearInvocations(metrics);
        setupAppenderMock(5);

        // The mock appender logic (below) writes actual data to the vector,
        // causing the underlying Arrow buffers to allocate memory.
        buffer.add(vsrAppender);

        long size = buffer.approximateSerializedBytes();
        assertTrue(size > 0, "Size should be greater than 0 after adding data");
    }

    @Test
    public void testCloseReleasesResources()
    {
        clearInvocations(metrics);
        setupAppenderMock(5);
        buffer.add(vsrAppender);

        // We can't easily peek inside Buffer to see if VSRs are closed without reflection,
        // but we can rely on the allocator to throw if we leak memory when the test tears down.
        // However, we can verify metrics interactions.

        buffer.close();

        verify(metrics).incBufferClosed();

        // Ensure buffer is effectively dead/empty
        assertEquals(buffer.getVsrCount(), 0);
    }

    private void setupAppenderMock(int rowsToAdd)
    {
        when(vsrAppender.getRowCount()).thenReturn(rowsToAdd);
        doAnswer(invocation -> {
            VectorSchemaRoot root = invocation.getArgument(0);
            int currentRows = root.getRowCount();
            int newRowCount = currentRows + rowsToAdd;

            // Simulate adding data so buffer size approximation works
            IntVector vector = (IntVector) root.getVector("col1");
            for (int i = currentRows; i < newRowCount; i++) {
                vector.setSafe(i, i * 10);
            }
            vector.setValueCount(newRowCount);

            root.setRowCount(newRowCount);
            return null;
        }).when(vsrAppender).append(any(VectorSchemaRoot.class));
    }
}
