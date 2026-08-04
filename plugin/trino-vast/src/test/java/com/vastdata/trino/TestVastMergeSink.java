/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastObjectDetails;
import com.vastdata.client.metrics.BufferedInsertMetrics;
import com.vastdata.client.metrics.ByColumnInserterMetrics;
import com.vastdata.client.metrics.RecordBatchSplitterMetrics;
import com.vastdata.trino.VastModule.InsertBufferAllocator;
import com.vastdata.trino.metrics.PageSinkMetrics;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.mockito.Mock;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static com.vastdata.client.buffering.BufferedRowIdBasedWriteTaskFactory.sortVsrByRowIdColumn;
import static com.vastdata.trino.VastSessionProperties.INSERT_BUFFER_MAX_REQUEST_BODY_SIZE;
import static com.vastdata.trino.VastSessionProperties.INSERT_BUFFER_OPEN_VSR_COUNT_PREALLOCATION;
import static com.vastdata.trino.VastSessionProperties.INSERT_BUFFER_OPEN_VSR_TARGET_ROW_COUNT;
import static com.vastdata.trino.VastSessionProperties.INSERT_BUFFER_SIZE_SOFT_LIMIT_IN_BYTES;
import static com.vastdata.trino.VastSessionProperties.INSERT_BUFFER_TARGET_ROW_COUNT_PER_PARTITION_FLUSH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

@TestInstance(Lifecycle.PER_CLASS)
public class TestVastMergeSink
{
    @Mock VastClient mockClient;
    @Mock ConnectorSession session;
    @Mock ConnectorPageSinkId pageSinkId;
    @Mock VastTableHandle tableHandle;
    @Mock BufferedInsertMetrics vastBufferedInsertMetrics;
    @Mock PageSinkMetrics pageSinkMetrics;
    @Mock VastObjectDetails vastObjectDetails;

    private AutoCloseable autoCloseable;
    private VastIoExecutor ioExecutor;
    private VastCpuExecutor cpuExecutor;
    private VastConfig config;

    @BeforeAll
    public void beforeClass()
    {
        config = new VastConfig();
        ioExecutor = new VastIoExecutor(config);
        cpuExecutor = new VastCpuExecutor(config);
    }

    @AfterAll
    public void afterClass()
    {
        ioExecutor.shutdown();
        cpuExecutor.shutdown();
    }

    @BeforeEach
    public void setup()
    {
        autoCloseable = openMocks(this);
    }

    @AfterEach
    public void tearDown()
            throws Exception
    {
        autoCloseable.close();
    }

    private VastMergeSink createDummyMergeSink()
    {
        InsertBufferAllocator insertBufferAllocator = new InsertBufferAllocator(
                new RootAllocator());
        VastPageSinkProvider pageSinkProvider = new VastPageSinkProvider(
                mockClient, config, insertBufferAllocator,
                new RecordBatchSplitterMetrics(), vastBufferedInsertMetrics,
                new ByColumnInserterMetrics(), pageSinkMetrics, ioExecutor,
                cpuExecutor);
        VastTransactionHandle transactionHandle = new VastTransactionHandle(1L);
        VastTableHandle vastTableHandle = new VastTableHandle("buck/schem",
                "tab", vastObjectDetails, false, false);
        VastMergeTableHandle mergeTableHandle = new VastMergeTableHandle(
                vastTableHandle, List.of());
        return (VastMergeSink) pageSinkProvider.createMergeSink(
                transactionHandle, session, mergeTableHandle, () -> 1);
    }

    @Test
    public void testRandomDataEndPoints()
    {
        {
            List<URI> uriList = IntStream.range(0, 10).mapToObj(
                    i -> URI.create("uri-" + i)).toList();
            when(session.getProperty("data_endpoints", List.class)).thenReturn(
                    uriList);
            when(session.getProperty("max_rows_per_insert",
                    Integer.class)).thenReturn(1000);
            when(session.getProperty("import_chunk_limit",
                    Integer.class)).thenReturn(1);

            when(session.getProperty(INSERT_BUFFER_OPEN_VSR_COUNT_PREALLOCATION,
                    Integer.class)).thenReturn(100);
            when(session.getProperty(
                    INSERT_BUFFER_TARGET_ROW_COUNT_PER_PARTITION_FLUSH,
                    Integer.class)).thenReturn(100);
            when(session.getProperty(INSERT_BUFFER_SIZE_SOFT_LIMIT_IN_BYTES,
                    Long.class)).thenReturn(100L);
            when(session.getProperty(INSERT_BUFFER_MAX_REQUEST_BODY_SIZE,
                    Long.class)).thenReturn(100L);
            when(session.getProperty(INSERT_BUFFER_OPEN_VSR_TARGET_ROW_COUNT,
                    Integer.class)).thenReturn(1);

            VastMergeSink mergeSink = createDummyMergeSink();
            VastMergeSink mergeSink1 = createDummyMergeSink();
            List<URI> shuffleUriNoSeed0 = mergeSink.getShuffledDataEndpoints();
            List<URI> shuffleUriNoSeed1 = mergeSink1.getShuffledDataEndpoints();
            assertNotEquals(shuffleUriNoSeed0, shuffleUriNoSeed1);

            when(session.getProperty(eq("seed_for_shuffling_endpoints"),
                    any())).thenReturn(123L);
            VastMergeSink mergeSink2 = createDummyMergeSink();
            VastMergeSink mergeSink3 = createDummyMergeSink();
            List<URI> shufflesUriWithSeed0 = mergeSink2.getShuffledDataEndpoints();
            List<URI> shufflesUriWithSeed1 = mergeSink3.getShuffledDataEndpoints();
            assertEquals(shufflesUriWithSeed0, shufflesUriWithSeed1);
            assertNotEquals(shufflesUriWithSeed0, uriList);

            when(session.getProperty("seed_for_shuffling_endpoints",
                    Long.class)).thenReturn(987L);
            VastMergeSink mergeSink4 = createDummyMergeSink();
            List<URI> shufflesUriWithSeed2 = mergeSink4.getShuffledDataEndpoints();
            assertNotEquals(shufflesUriWithSeed0, shufflesUriWithSeed2);
        }
    }

    @Test
    public void testListVectorTypeWithFixedSizeBinaryCopy()
    {
        try (RootAllocator allocator = new RootAllocator()) {
            UInt8Vector rowIdVector = new UInt8Vector("rowidvector", allocator);
            rowIdVector.allocateNew(2);
            rowIdVector.setSafe(0, 1);
            rowIdVector.setSafe(1, 0);
            rowIdVector.setValueCount(2);

            ListVector complexVector = ListVector.empty("complex", allocator);
            FixedSizeBinaryVector dataVector = (FixedSizeBinaryVector) complexVector
                    .addOrGetVector(FieldType.notNullable(
                            new ArrowType.FixedSizeBinary(36)))
                    .getVector();
            complexVector.allocateNew();

            byte[] byeBytes = Arrays.copyOf(
                    "bye".getBytes(StandardCharsets.UTF_8), 36);
            complexVector.startNewValue(0);
            dataVector.setSafe(0, byeBytes);
            complexVector.endValue(0, 1);

            byte[] hiBytes = Arrays.copyOf(
                    "hi".getBytes(StandardCharsets.UTF_8), 36);
            complexVector.startNewValue(1);
            dataVector.setSafe(1, hiBytes);
            complexVector.endValue(1, 1);

            dataVector.setValueCount(2);
            complexVector.setValueCount(2);

            try (VectorSchemaRoot sourceVsr = new VectorSchemaRoot(
                    List.of(rowIdVector, complexVector))) {
                ListVector sourceComplexVector = (ListVector) sourceVsr.getVector(
                        "complex");

                List<?> lista0 = sourceComplexVector.getObject(0);
                assertEquals("bye", new String((byte[]) lista0.get(0),
                        StandardCharsets.UTF_8).trim());

                List<?> lista1 = sourceComplexVector.getObject(1);
                assertEquals("hi", new String((byte[]) lista1.get(0),
                        StandardCharsets.UTF_8).trim());

                try (VectorSchemaRoot targetVsr = sortVsrByRowIdColumn(
                        sourceVsr, allocator)) {
                    ListVector targetComplexVector = (ListVector) targetVsr.getVector(
                            "complex");

                    List<?> list0 = targetComplexVector.getObject(0);
                    assertEquals("hi", new String((byte[]) list0.get(0),
                            StandardCharsets.UTF_8).trim());

                    List<?> list1 = targetComplexVector.getObject(1);
                    assertEquals("bye", new String((byte[]) list1.get(0),
                            StandardCharsets.UTF_8).trim());
                }
            }
        }
    }

    @Test
    public void testMapVectorTypeWithFixedSizeBinaryCopy()
    {
        try (RootAllocator allocator = new RootAllocator()) {
            UInt8Vector rowIdVector = new UInt8Vector("rowidvector", allocator);
            rowIdVector.allocateNew(1);
            rowIdVector.setSafe(0, 0);
            rowIdVector.setValueCount(1);

            MapVector complexVector = MapVector.empty("complex", allocator,
                    false);
            StructVector entriesVector = (StructVector) complexVector
                    .addOrGetVector(
                            FieldType.notNullable(new ArrowType.Struct()))
                    .getVector();
            FixedSizeBinaryVector keyVector = (FixedSizeBinaryVector) entriesVector.addOrGet(
                    "key",
                    FieldType.notNullable(new ArrowType.FixedSizeBinary(36)),
                    FixedSizeBinaryVector.class);
            FixedSizeBinaryVector valueVector = (FixedSizeBinaryVector) entriesVector.addOrGet(
                    "value",
                    FieldType.nullable(new ArrowType.FixedSizeBinary(36)),
                    FixedSizeBinaryVector.class);
            complexVector.allocateNew();

            byte[] byeKey = Arrays.copyOf(
                    "bye_k".getBytes(StandardCharsets.UTF_8), 36);
            byte[] byeVal = Arrays.copyOf(
                    "bye_v".getBytes(StandardCharsets.UTF_8), 36);
            complexVector.startNewValue(0);
            entriesVector.setIndexDefined(0);
            keyVector.setSafe(0, byeKey);
            valueVector.setSafe(0, byeVal);
            complexVector.endValue(0, 1);

            keyVector.setValueCount(1);
            valueVector.setValueCount(1);
            entriesVector.setValueCount(1);
            complexVector.setValueCount(1);

            try (VectorSchemaRoot sourceVsr = new VectorSchemaRoot(
                    List.of(rowIdVector, complexVector))) {
                MapVector sourceComplexVector = (MapVector) sourceVsr.getVector(
                        "complex");
                StructVector sourceEntriesVector = (StructVector) sourceComplexVector.getDataVector();
                FixedSizeBinaryVector sourceKeyVector = (FixedSizeBinaryVector) sourceEntriesVector.getChild(
                        "key");
                FixedSizeBinaryVector sourceValueVector = (FixedSizeBinaryVector) sourceEntriesVector.getChild(
                        "value");

                assertEquals("bye_k", new String(sourceKeyVector.get(0),
                        StandardCharsets.UTF_8).trim());
                assertEquals("bye_v", new String(sourceValueVector.get(0),
                        StandardCharsets.UTF_8).trim());

                try (VectorSchemaRoot targetVsr = sortVsrByRowIdColumn(
                        sourceVsr, allocator)) {
                    MapVector targetComplexVector = (MapVector) targetVsr.getVector(
                            "complex");
                    StructVector targetEntriesVector = (StructVector) targetComplexVector.getDataVector();
                    FixedSizeBinaryVector targetKeyVector = (FixedSizeBinaryVector) targetEntriesVector.getChild(
                            "key");
                    FixedSizeBinaryVector targetValueVector = (FixedSizeBinaryVector) targetEntriesVector.getChild(
                            "value");

                    assertEquals("bye_k", new String(targetKeyVector.get(0),
                            StandardCharsets.UTF_8).trim());
                    assertEquals("bye_v", new String(targetValueVector.get(0),
                            StandardCharsets.UTF_8).trim());
                }
            }
        }
    }
}
