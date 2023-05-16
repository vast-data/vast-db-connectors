/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.vastdata.client.error.VastTooLargePageException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.VastPayloadSerializer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRecordBatchSplitter
{
    private static final BufferAllocator allocator = new RootAllocator();

    @Test
    public void testSingleSplit()
            throws VastUserException
    {
        RecordBatchSplitter splitter = new RecordBatchSplitter(Long.MAX_VALUE, VastPayloadSerializer.getInstanceForRecordBatch());
        try (VectorSchemaRoot root = createRecordBatch(4096, 10000)) {
            List<byte[]> parts = splitter.split(root, Integer.MAX_VALUE);
            assertThat(parts.size()).isEqualTo(1);
        }
    }

    @Test
    public void testMultipleSplits()
            throws VastUserException
    {
        RecordBatchSplitter splitter = new RecordBatchSplitter(1000 * 1000, VastPayloadSerializer.getInstanceForRecordBatch());
        try (VectorSchemaRoot root = createRecordBatch(4096, 10000)) {
            List<byte[]> parts = splitter.split(root, Integer.MAX_VALUE);
            assertThat(parts.size()).isGreaterThan(1);
        }
        try (VectorSchemaRoot root = createRecordBatch(4096, 8)) {
            List<byte[]> parts = splitter.split(root, 1024);
            assertThat(parts.size()).isEqualTo(4);
        }
    }

    @Test(expectedExceptions = VastTooLargePageException.class, expectedExceptionsMessageRegExp = "Failed to serialize too large RecordBatch.*")
    public void testTooSmallLimit()
            throws VastUserException
    {
        RecordBatchSplitter splitter = new RecordBatchSplitter(0, VastPayloadSerializer.getInstanceForRecordBatch());
        try (VectorSchemaRoot root = createRecordBatch(4096, 10000)) {
            splitter.split(root, Integer.MAX_VALUE);
        }
    }

    @Test
    public void testNoSplits()
            throws VastUserException
    {
        RecordBatchSplitter splitter = new RecordBatchSplitter(1000 * 1000, VastPayloadSerializer.getInstanceForRecordBatch());
        try (VectorSchemaRoot root = createRecordBatch(0, 10000)) {
            List<byte[]> parts = splitter.split(root, Integer.MAX_VALUE);
            assertThat(parts.size()).isEqualTo(0);
        }
    }

    private static VectorSchemaRoot createRecordBatch(int rows, int rowSize)
    {
        VarCharVector vector = new VarCharVector("v", allocator);
        byte[] text = new byte[rowSize];
        Arrays.fill(text, (byte) 'a');
        IntStream.range(0, rows).forEach(i -> vector.setSafe(i, text));

        VectorSchemaRoot root = VectorSchemaRoot.of(vector);
        root.setRowCount(rows);
        return root;
    }
}
