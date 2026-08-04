/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.buffering.VsrAppender;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPageToVsr
{
    private static final Schema INT_SCHEMA = new Schema(List.of(new Field("f1",
            FieldType.nullable(new ArrowType.Int(32, true)), List.of())));

    private static VectorSchemaRoot getOrderedPageForValidation(Page page,
            BiFunction<Page, Integer, Long> rowBufferAssigner,
            BufferAllocator allocator, Schema schema)
    {
        List<Map.Entry<Long, List<Integer>>> partitions = new ArrayList<>(
                IntStream
                        .range(0, page.getPositionCount())
                        .boxed()
                        .collect(Collectors.groupingBy(
                                i -> rowBufferAssigner.apply(page, i)))
                        .entrySet());

        int[] orderedPositions = partitions
                .stream()
                .flatMap(e -> e.getValue().stream())
                .mapToInt(Integer::intValue)
                .toArray();

        VastRecordBatchBuilder builder = new VastRecordBatchBuilder(schema,
                allocator);
        return builder.build(page.copyPositions(orderedPositions, 0,
                orderedPositions.length));
    }

    @Test
    public void testGetBufferIdToPage_SinglePartition_MemoryLifecycle()
            throws Exception
    {
        try (RootAllocator allocator = new RootAllocator()) {
            BiFunction<Page, Integer, Long> rowBufferAssigner = (page, row) -> 100L;

            // Create a page with 10 integers
            Block block = new IntArrayBlock(10, Optional.empty(),
                    new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
            Page page = new Page(block);

            Map<Long, VsrAppender> result = PageToVsr.getPageToBufferVsrAppender(page, rowBufferAssigner, allocator, INT_SCHEMA);

            // Verify Structure
            assertEquals(1, result.size());
            assertTrue(result.containsKey(100L));
            VsrAppender appender = result.get(100L);
            assertEquals(10, appender.getRowCount());
            assertEquals(INT_SCHEMA, appender.getSchema());

            // Verify Memory Allocation
            long memoryAfterBuild = allocator.getAllocatedMemory();
            assertEquals(0, memoryAfterBuild, "Expected no memory to be allocated for the VSR before append");

            // Verify Append
            try (VectorSchemaRoot target = VectorSchemaRoot.create(
                        INT_SCHEMA, allocator)) {
                appender.append(target);
                assertEquals(10, target.getRowCount());
                assertEquals(0, ((IntVector) target.getVector(0)).get(0));
                assertEquals(9, ((IntVector) target.getVector(0)).get(9));

                appender.close();
            }
        }
    }

    @Test
    public void testGetBufferIdToPage_MultiPartition()
            throws Exception
    {
        try (RootAllocator allocator = new RootAllocator()) {
            // Force rows to split: Even -> Partition 10, Odd -> Partition 20
            BiFunction<Page, Integer, Long> rowBufferAssigner = (page, row) ->
                    row % 2 == 0 ?
                            10L :
                            20L;

            // Create a page with 4 rows: 0 (even), 1 (odd), 2 (even), 3 (odd)
            Block block = new IntArrayBlock(4, Optional.empty(),
                    new int[] {100, 101, 102, 103});
            Page page = new Page(block);

            // Execute
            Map<Long, VsrAppender> result = PageToVsr.getPageToBufferVsrAppender(
                    page, rowBufferAssigner, allocator, INT_SCHEMA);

            // Verify Structure
            assertEquals(2, result.size());
            VsrAppender appenderEven = result.get(10L);
            VsrAppender appenderOdd = result.get(20L);

            assertEquals(2, appenderEven.getRowCount()); // 2 even rows
            assertEquals(2, appenderOdd.getRowCount());  // 2 odd rows

            // Verify Memory Allocation - should be 0 before append
            long initialMemory = allocator.getAllocatedMemory();
            assertEquals(0, initialMemory);

            try (VectorSchemaRoot rootEven = VectorSchemaRoot.create(INT_SCHEMA, allocator);
                    VectorSchemaRoot rootOdd = VectorSchemaRoot.create(INT_SCHEMA, allocator)) {
                // Append data to targets
                appenderEven.append(rootEven);
                appenderOdd.append(rootOdd);

                // Memory increased because of targets
                long memoryWithTargets = allocator.getAllocatedMemory();
                assertTrue(memoryWithTargets > initialMemory);

                appenderEven.close();
                appenderOdd.close();

                // Assert correctness (same data as expected) after all the appenders closed
                assertEquals(2, rootEven.getRowCount());
                assertEquals(100, ((IntVector) rootEven.getVector(0)).get(0));
                assertEquals(102, ((IntVector) rootEven.getVector(0)).get(1));

                assertEquals(2, rootOdd.getRowCount());
                assertEquals(101, ((IntVector) rootOdd.getVector(0)).get(0));
                assertEquals(103, ((IntVector) rootOdd.getVector(0)).get(1));
            }
        }
    }

    @Test
    public void testGetBufferIdToPage_EmptyPage()
            throws Exception
    {
        try (RootAllocator allocator = new RootAllocator()) {
            BiFunction<Page, Integer, Long> rowBufferAssigner = (page, row) -> 1L;

            Page emptyPage = new Page(
                    new IntArrayBlock(0, Optional.empty(), new int[0]));

            Map<Long, VsrAppender> result = PageToVsr.getPageToBufferVsrAppender(
                    emptyPage, rowBufferAssigner, allocator, INT_SCHEMA);

            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testVarcharPartitioning()
    {
        try (BufferAllocator allocator = new RootAllocator()) {
            // 1. Setup Arrow Schema (1 VARCHAR column)
            Field varcharField = new Field("col0",
                    FieldType.nullable(new ArrowType.Utf8()), null);
            Schema schema = new Schema(List.of(varcharField));

            // 2. Setup Trino Page (3 rows: "apple", "banana", "cherry")
            byte[] data = "applebananacherry".getBytes(StandardCharsets.UTF_8);
            Block varcharBlock = new VariableWidthBlock(3,
                    Slices.wrappedBuffer(data), new int[] {0, 5, 11, 17},
                    Optional.empty());
            Page page = new Page(3, varcharBlock);

            byte[] firstPartitionData = "applecherry".getBytes(
                    StandardCharsets.UTF_8);
            Block firstPartitionVarcharBlock = new VariableWidthBlock(2,
                    Slices.wrappedBuffer(firstPartitionData),
                    new int[] {0, 5, 11}, Optional.empty());
            Page firstPartitionPage = new Page(2, firstPartitionVarcharBlock);

            // 3. Row Assigner: Rows 0 and 2 -> Buffer 100, Row 1 -> Buffer 200
            BiFunction<Page, Integer, Long> rowBufferAssigner = (p, i) -> (i % 2 == 0) ?
                    100L :
                    200L;
            Map<Long, VsrAppender> appenders = PageToVsr.getPageToBufferVsrAppender(
                    page, rowBufferAssigner, allocator, schema);

            // 4. Assertions
            assertEquals(2, appenders.size());
            assertEquals(2, appenders.get(100L).getRowCount(),
                    "Buffer 100 should have 'apple' and 'cherry'");
            assertEquals(1, appenders.get(200L).getRowCount(),
                    "Buffer 200 should have 'banana'");

            try (VectorSchemaRoot sortedForValidation = getOrderedPageForValidation(
                    firstPartitionPage, rowBufferAssigner, allocator, schema)) {
                // 5. Verify Content of Buffer 100
                try (VectorSchemaRoot target = VectorSchemaRoot.create(schema,
                        allocator)) {
                    appenders.get(100L).append(target);
                    VarCharVector vector = (VarCharVector) target.getVector(0);

                    assertEquals("apple",
                            new String(vector.get(0), StandardCharsets.UTF_8));
                    assertEquals("cherry",
                            new String(vector.get(1), StandardCharsets.UTF_8));

                    appenders.values().forEach(VsrAppender::close);

                    assertVsrEquals(sortedForValidation, target);
                }
            }
        }
    }

    @Test
    public void testInterleavedRowsOrderingAndContent()
    {
        try (RootAllocator allocator = new RootAllocator()) {
            // Values: [10, 20, 30, 40]
            // Indexes: 0, 1, 2, 3
            // Partition A: 0, 2 -> [10, 30]
            // Partition B: 1, 3 -> [20, 40]
            Block block = new IntArrayBlock(4, Optional.empty(),
                    new int[] {10, 20, 30, 40});
            Page page = new Page(block);

            BiFunction<Page, Integer, Long> assigner = (p, row) -> (row % 2 == 0) ?
                    1000L :
                    2000L;

            Map<Long, VsrAppender> result = PageToVsr.getPageToBufferVsrAppender(
                    page, assigner, allocator, INT_SCHEMA);

            VsrAppender appenderA = result.get(1000L);
            VsrAppender appenderB = result.get(2000L);

            // Verify content of A
            try (VectorSchemaRoot rootA = VectorSchemaRoot.create(INT_SCHEMA,
                    allocator)) {
                appenderA.append(rootA);
                assertEquals(2, rootA.getRowCount());
                IntVector vec = (IntVector) rootA.getVector(0);
                assertEquals(10, vec.get(0));
                assertEquals(30, vec.get(1));
            }

            // Verify content of B
            try (VectorSchemaRoot rootB = VectorSchemaRoot.create(INT_SCHEMA,
                    allocator)) {
                appenderB.append(rootB);
                assertEquals(2, rootB.getRowCount());
                IntVector vec = (IntVector) rootB.getVector(0);
                assertEquals(20, vec.get(0));
                assertEquals(40, vec.get(1));
            }

            result.values().forEach(VsrAppender::close);
        }
    }

    @Test
    public void testAppendToUninitializedRoot()
    {
        try (RootAllocator allocator = new RootAllocator()) {
            Block block = new IntArrayBlock(1, Optional.empty(),
                    new int[] {42});
            Page page = new Page(block);
            Map<Long, VsrAppender> result = PageToVsr.getPageToBufferVsrAppender(
                    page, (p, i) -> 1L, allocator, INT_SCHEMA);
            VsrAppender appender = result.get(1L);

            try (VectorSchemaRoot target = VectorSchemaRoot.create(INT_SCHEMA,
                    allocator)) {
                // Do NOT call allocateNew() on target
                assertEquals(0, target.getRowCount());

                appender.append(target);

                assertEquals(1, target.getRowCount());
                assertEquals(42, ((IntVector) target.getVector(0)).get(0));
            }
            appender.close();
        }
    }

    @Test
    public void testAppendSchemaMismatch()
    {
        try (RootAllocator allocator = new RootAllocator()) {
            Block block = new IntArrayBlock(1, Optional.empty(), new int[] {1});
            Page page = new Page(block);
            Map<Long, VsrAppender> result = PageToVsr.getPageToBufferVsrAppender(
                    page, (p, i) -> 1L, allocator, INT_SCHEMA);
            VsrAppender appender = result.get(1L);

            // Create a target with a different schema (BigInt instead of Int)
            Schema mismatchSchema = new Schema(List.of(new Field("f1",
                    FieldType.nullable(new ArrowType.Int(64, true)),
                    List.of())));

            try (VectorSchemaRoot target = VectorSchemaRoot.create(
                    mismatchSchema, allocator)) {
                target.allocateNew();
                // Expect exception due to type mismatch
                assertThrows(IllegalArgumentException.class,
                        () -> appender.append(target));
            }
            appender.close();
        }
    }

    private void assertVsrEquals(VectorSchemaRoot expected,
            VectorSchemaRoot actual)
    {
        // 1. Compare Schemas
        assertEquals(expected.getSchema(), actual.getSchema(),
                "Schemas do not match");

        // 2. Compare Row Counts
        assertEquals(expected.getRowCount(), actual.getRowCount(),
                "Row counts do not match");

        // 3. Compare Data Vectors
        List<FieldVector> expectedVectors = expected.getFieldVectors();
        List<FieldVector> actualVectors = actual.getFieldVectors();

        assertEquals(expectedVectors.size(), actualVectors.size(),
                "Number of columns do not match");

        for (int i = 0; i < expectedVectors.size(); i++) {
            FieldVector v1 = expectedVectors.get(i);
            FieldVector v2 = actualVectors.get(i);

            assertTrue(VectorEqualsVisitor.vectorEquals(v1, v2),
                    "Content mismatch in column: " + v1.getName());
        }
    }
}
