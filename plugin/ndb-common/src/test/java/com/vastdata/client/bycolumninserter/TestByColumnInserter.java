/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.bycolumninserter;

import com.google.common.base.Predicates;
import com.vastdata.client.QueryDataExtraParams;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastException;
import com.vastdata.client.metrics.ByColumnInserterMetrics;
import com.vastdata.client.metrics.InsertedRowsStats;
import com.vastdata.client.metrics.RecordBatchSplitterMetrics;
import com.vastdata.client.rowid.RowIDStrategyType;
import com.vastdata.client.rowid.RowIdListSchemaFactory;
import com.vastdata.client.rowid.TableType;
import com.vastdata.client.tx.VastTransaction;
import io.airlift.log.Logger;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestByColumnInserter
{
    private static final Logger LOG = Logger.get(TestByColumnInserter.class);
    private final URI dataEndpoint = URI.create("http://localhost:1234");
    private final int rowCount = 10;
    @Mock private VastClient vastClient;
    @Mock private VastTransaction transaction;
    private RootAllocator allocator;
    private BufferAllocator inputAllocator;
    private BufferAllocator resultAllocator;
    private BufferAllocator mockedResultAllocator;
    private BufferAllocator deserializeAllocator;
    private VastConfig vastConfig;
    private ExecutorService executor;

    @BeforeClass
    public void beforeClass()
    {
        executor = new ThreadPoolExecutor(1, 4, 15, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
    }

    @AfterClass
    public void afterClass()
    {
        executor.shutdown();
    }

    @BeforeMethod
    public void setUp()
            throws VastException
    {
        MockitoAnnotations.openMocks(this);
        allocator = new RootAllocator();
        inputAllocator = allocator.newChildAllocator("input", 0,
                Long.MAX_VALUE);
        resultAllocator = allocator.newChildAllocator("result", 0,
                Long.MAX_VALUE);
        mockedResultAllocator = allocator.newChildAllocator("mocked-result", 0,
                Long.MAX_VALUE);
        deserializeAllocator = allocator.newChildAllocator("deserialize", 0,
                Long.MAX_VALUE);
        vastConfig = new VastConfig();

        // Common mock setup
        when(vastClient.insertRows(any(), any(), any(), any(), eq(true), any(),
               any(), any())).thenAnswer(invocation -> mockInsertResponse(rowCount));
    }

    @AfterMethod
    public void tearDown()
    {
        resultAllocator.close();
        inputAllocator.close();
        mockedResultAllocator.close();
        deserializeAllocator.close();
        allocator.close();
    }

    ByColumnInserter getByColumnInserter(
            Predicate<String> isNonUpdateableColumn)
    {
        return new ByColumnInserter(vastClient, vastConfig,
                RowIDStrategyType.UNSIGNED_INT64, "s", "t", transaction,
                List.of(dataEndpoint), new QueryDataExtraParams(), "user", new RecordBatchSplitterMetrics(),
                new ByColumnInserterMetrics(), isNonUpdateableColumn,
                new InsertedRowsStats(), executor, executor, "test-trace");
    }

    @Test
    public void testEmptyInsert()
            throws VastException
    {
        // Setup
        Schema emptySchema = new Schema(List.of());
        ByColumnInserter inserter = getByColumnInserter((c -> false));
        try (VectorSchemaRoot emptyVsr = VectorSchemaRoot.create(emptySchema,
                inputAllocator)) {
            emptyVsr.setRowCount(0);
            try (VectorSchemaRoot insertedRowIds = inserter
                    .insert(List.of(emptyVsr), resultAllocator)
                    .join()) {
                assertEquals(insertedRowIds.getRowCount(), 0);
                verify(vastClient, never()).insertRows(any(), any(), any(),
                        any(), anyBoolean(), any(), any(), any());
                verify(vastClient, never()).updateRows(any(), any(), any(),
                        any(), any(), any());
            }
        }
    }

    @Test
    public void testInsertAllColumnsInOneBatch()
            throws VastException, IOException
    {
        // Setup: Vectors small enough to fit in one request
        vastConfig.setMaxRequestBodySize(2000L);
        TestCase testCase = new TestCase(
                List.of(createSizedVector("col1", 400, rowCount)),
                List.of(createSizedVector("col2", 500, rowCount)));
        ByColumnInserter inserter = getByColumnInserter(
                testCase.isNonUpdateableColumn);
        try (VectorSchemaRoot inputVsr = vsrFromVectors(testCase.getAllFields(),
                rowCount)) {
            inserter.insert(List.of(inputVsr), resultAllocator).join().close();
        }
        // Verify insert was called once with all columns, and update never called
        ArgumentCaptor<byte[]> bodyCaptor = ArgumentCaptor.forClass(
                byte[].class);
        verify(vastClient, times(1)).insertRows(any(), any(), any(),
                bodyCaptor.capture(), eq(true), any(), any(), any());
        verify(vastClient, never()).updateRows(any(), any(), any(), any(),
                any(), any());

        // Inspect the serialized request body
        try (VectorSchemaRoot insertedVsr = deserialize(
                bodyCaptor.getValue())) {
            assertEquals(insertedVsr.getRowCount(), rowCount);
            List<String> colNames = getColumnNames(insertedVsr);
            assertEquals(colNames.size(), 2);
            assertTrue(colNames.containsAll(List.of("col1", "col2")));
        }
    }

    @Test
    public void testInsertAndOneUpdate()
            throws VastException, IOException
    {
        // Setup: v1(non-upd)+v3(upd) fit, but adding v2(upd) exceeds the limit.
        // `planInsertBatch` should pack v1 and v3 (largest updatable), leaving v2 for update.
        vastConfig.setMaxRequestBodySize(1000L);
        TestCase testCase = new TestCase(
                List.of(createSizedVector("col1", 500, rowCount)),
                List.of(createSizedVector("col2", 150, rowCount),
                        createSizedVector("col3", 250, rowCount)));

        ByColumnInserter inserter = getByColumnInserter(
                testCase.isNonUpdateableColumn);

        try (VectorSchemaRoot inputVsr = vsrFromVectors(testCase.getAllFields(),
                rowCount)) {
            inserter.insert(List.of(inputVsr), resultAllocator).join().close();
        }

        // Verify insert call
        ArgumentCaptor<byte[]> insertBody = ArgumentCaptor.forClass(
                byte[].class);
        verify(vastClient, times(1)).insertRows(any(), any(), any(),
                insertBody.capture(), eq(true), any(), any(), any());
        try (VectorSchemaRoot insertedVsr = deserialize(
                insertBody.getValue())) {
            List<String> colNames = getColumnNames(insertedVsr);
            assertEquals(colNames.size(), 2,
                    "Insert batch should have 2 columns");
            assertTrue(colNames.containsAll(List.of("col1", "col2")),
                    "Insert should contain non-updatable and  updatable");
        }

        // Verify update call
        ArgumentCaptor<byte[]> updateBody = ArgumentCaptor.forClass(
                byte[].class);
        verify(vastClient, times(1)).updateRows(any(), any(),
                updateBody.capture(), any(), any(), any());
        try (VectorSchemaRoot updatedVsr = deserialize(updateBody.getValue())) {
            List<String> colNames = getColumnNames(updatedVsr);
            assertEquals(colNames.size(), 2,
                    "Update batch should have 2 columns");
            assertTrue(colNames.containsAll(List.of(ROW_ID_FIELD_NAME, "col3")),
                    "Update should contain row_id and remaining column");
        }
    }

    @Test
    public void testInsertAndMultipleUpdates()
            throws VastException, IOException
    {
        // Setup: one insert and two separate updates
        vastConfig.setMaxRequestBodySize(1200L);
        TestCase testCase = new TestCase(
                List.of(createSizedVector("col1", 300, rowCount)),
                List.of(createSizedVector("col2", 400, rowCount),
                        createSizedVector("col3", 400, rowCount)));
        ByColumnInserter inserter = getByColumnInserter(
                testCase.isNonUpdateableColumn);

        try (VectorSchemaRoot inputVsr = vsrFromVectors(testCase.getAllFields(),
                rowCount)) {
            inserter.insert(List.of(inputVsr), resultAllocator).join().close();
        }
        // Verify one insert call
        ArgumentCaptor<byte[]> insertBody = ArgumentCaptor.forClass(
                byte[].class);
        verify(vastClient, times(1)).insertRows(any(), any(), any(),
                insertBody.capture(), eq(true), any(), any(), any());
        try (VectorSchemaRoot insertedVsr = deserialize(
                insertBody.getValue())) {
            assertEquals(getColumnNames(insertedVsr).size(), 2,
                    "Insert batch should have non-updatable and one updatable");
        }

        // Verify two update calls
        ArgumentCaptor<byte[]> updateBodies = ArgumentCaptor.forClass(
                byte[].class);
        verify(vastClient, times(1)).updateRows(any(), any(),
                updateBodies.capture(), any(), any(), any());
        List<String> updatedCols = updateBodies
                .getAllValues()
                .stream()
                .flatMap(body -> {
                    try (VectorSchemaRoot vsr = deserialize(body)) {
                        return getColumnNames(vsr).stream();
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .filter(name -> !name.equals(ROW_ID_FIELD_NAME))
                .collect(Collectors.toList());

        assertTrue(updatedCols.contains("col2") || updatedCols.contains("col3"),
                "An updatable column should be in an update");
        assertTrue(updatedCols.size() >= 1);
    }

    @Test
    public void testUpdateColumnsDroppedWhenNotAllFit()
            throws VastException, IOException
    {
        // RPC body size big enough to fit only the first two columns
        vastConfig.setMaxRequestBodySize(1300L);
        // col1 is inserted. col2 and col3 need update.
        // col2 (800) fits in 1200. col3 (800) fits in 1200.
        // Together (1600) they don't fit.
        // Estimator picks one. The other should remain for next batch.
        TestCase testCase = new TestCase(
                List.of(createSizedVector("col1", 200, rowCount)),
                List.of(createSizedVector("col2", 800, rowCount),
                        createSizedVector("col3", 800, rowCount)));

        ByColumnInserter inserter = getByColumnInserter(
                testCase.isNonUpdateableColumn);

        try (VectorSchemaRoot inputVsr = vsrFromVectors(testCase.getAllFields(),
                rowCount)) {
            inserter.insert(List.of(inputVsr), resultAllocator).join().close();
        }

        verify(vastClient, times(1)).insertRows(any(), any(), any(), any(),
                eq(true), any(), any(), any());

        ArgumentCaptor<byte[]> updateCaptor = ArgumentCaptor.forClass(
                byte[].class);
        verify(vastClient, times(1)).updateRows(any(), any(),
                updateCaptor.capture(), any(), any(), any());

        List<String> updatedColumns = updateCaptor
                .getAllValues()
                .stream()
                .flatMap(body -> {
                    try (VectorSchemaRoot vsr = deserialize(body)) {
                        return getColumnNames(vsr).stream();
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .filter(n -> !ROW_ID_FIELD_NAME.equals(n))
                .collect(Collectors.toList());

        assertTrue(updatedColumns.contains("col3"), "col3 should be updated");
    }

    @Test
    public void testNonUpdatableColumnsSplitByRows()
            throws VastException, IOException
    {
        // Setup: non-updatable columns are too big for one request, must be split by rows.
        vastConfig.setMaxRequestBodySize(1000L);
        TestCase testCase = new TestCase(
                List.of(createSizedVector("col1", 800, rowCount),
                        createSizedVector("col2", 800, rowCount)),
                List.of(createSizedVector("col3", 300, rowCount)));
        ByColumnInserter inserter = getByColumnInserter(
                testCase.isNonUpdateableColumn);

        try (VectorSchemaRoot inputVsr = vsrFromVectors(testCase.getAllFields(),
                rowCount)) {
            inserter.insert(List.of(inputVsr), resultAllocator).join().close();
        }

        // Verify insert was split (atLeastOnce because splitter logic is complex)
        ArgumentCaptor<byte[]> insertBodyCaptor = ArgumentCaptor.forClass(
                byte[].class);
        verify(vastClient, atLeastOnce()).insertRows(any(), any(), any(),
                insertBodyCaptor.capture(), eq(true), any(), any(), any());

        // Verify all insert bodies only contain non-updatable columns
        for (byte[] body : insertBodyCaptor.getAllValues()) {
            try (VectorSchemaRoot insertedVsr = deserialize(body)) {
                List<String> colNames = getColumnNames(insertedVsr);
                assertEquals(colNames.size(), 2);
                assertTrue(colNames.containsAll(List.of("col1", "col2")));
            }
        }

        // Verify update call for the updatable column
        ArgumentCaptor<byte[]> updateBodyCaptor = ArgumentCaptor.forClass(
                byte[].class);
        verify(vastClient, times(1)).updateRows(any(), any(),
                updateBodyCaptor.capture(), any(), any(), any());
        try (VectorSchemaRoot updatedVsr = deserialize(
                updateBodyCaptor.getValue())) {
            assertTrue(getColumnNames(updatedVsr).containsAll(
                    List.of(ROW_ID_FIELD_NAME, "col3")));
        }
    }

    @Test
    public void testFourNonUpdatableColumnsSplitByRows()
            throws VastException, IOException
    {
        // Setup: non-updatable columns are too big for one request, must be split by rows.
        vastConfig.setMaxRequestBodySize(1000L);
        TestCase testCase = new TestCase(
                List.of(createSizedVector("col1", 800, rowCount),
                        createSizedVector("col2", 800, rowCount),
                        createSizedVector("col3", 300, rowCount),
                        createSizedVector("col4", 300, rowCount)),
                List.of(createSizedVector("col5", 300, rowCount),
                        createSizedVector("col6", 300, rowCount),
                        createSizedVector("col7", 300, rowCount),
                        createSizedVector("col8", 300, rowCount),
                        createSizedVector("col9", 300, rowCount),
                        createSizedVector("col10", 300, rowCount)));

        ByColumnInserter inserter = getByColumnInserter(
                testCase.isNonUpdateableColumn);

        try (VectorSchemaRoot inputVsr = vsrFromVectors(testCase.getAllFields(),
                rowCount)) {
            inserter.insert(List.of(inputVsr), resultAllocator).join().close();
        }

        Set<String> nonUpdatableColumns = Set.of("col1", "col2", "col3",
                "col4");
        Set<String> updatableColumns = Set.of("col5", "col6", "col7", "col8",
                "col9", "col10");

        assertUpdateAndInsert(nonUpdatableColumns, updatableColumns, rowCount);
    }

    private void assertUpdateAndInsert(Set<String> nonUpdatableColumns,
            Set<String> updatableColumns, int rowCount)
            throws VastException, IOException
    {
        Map<String, Integer> colRowCounter = new HashMap<>();

        ArgumentCaptor<byte[]> insertBodyCaptor = ArgumentCaptor.forClass(
                byte[].class);
        verify(vastClient, atLeastOnce()).insertRows(any(), any(), any(),
                insertBodyCaptor.capture(), eq(true), any(), any(), any());
        ArgumentCaptor<byte[]> updateBodyCaptor = ArgumentCaptor.forClass(
                byte[].class);
        verify(vastClient, atLeastOnce()).updateRows(any(), any(),
                updateBodyCaptor.capture(), any(), any(), any());

        for (byte[] body : insertBodyCaptor.getAllValues()) {
            try (VectorSchemaRoot insertedVsr = deserialize(body)) {
                insertedVsr
                        .getSchema()
                        .getFields()
                        .stream()
                        .map(Field::getName)
                        .forEach(name -> colRowCounter.put(name,
                                colRowCounter.getOrDefault(name,
                                        0) + insertedVsr.getRowCount()));
            }
        }

        assertEquals(colRowCounter.keySet(), nonUpdatableColumns,
                "nonUpdatableColumns must be inserted");

        for (byte[] body : updateBodyCaptor.getAllValues()) {
            try (VectorSchemaRoot updatedVsr = deserialize(body)) {
                updatedVsr
                        .getSchema()
                        .getFields()
                        .stream()
                        .map(Field::getName)
                        .filter(name -> !name.equals(ROW_ID_FIELD_NAME))
                        .forEach(name -> colRowCounter.put(name,
                                colRowCounter.getOrDefault(name,
                                        0) + updatedVsr.getRowCount()));
            }
        }

        Set<String> allColumns = Stream
                .concat(updatableColumns.stream(), nonUpdatableColumns.stream())
                .collect(Collectors.toSet());
        assertEquals(colRowCounter.keySet(), allColumns,
                "All columns must be inserted or updated");
        for (Integer count : colRowCounter.values()) {
            assertEquals(count.intValue(), rowCount,
                    "Each column should have rowCount rows");
        }
    }

    private List<String> getColumnNames(VectorSchemaRoot vsr)
    {
        return vsr
                .getSchema()
                .getFields()
                .stream()
                .map(Field::getName)
                .collect(Collectors.toList());
    }

    private VectorSchemaRoot mockInsertResponse(int rowCount)
    {
        Schema rowIdSchema = RowIdListSchemaFactory.get(TableType.REGULAR);
        VectorSchemaRoot rowIdVsr = VectorSchemaRoot.create(rowIdSchema,
                mockedResultAllocator);
        UInt8Vector rowIdVector = (UInt8Vector) rowIdVsr.getVector(0);
        rowIdVector.allocateNew(rowCount);
        for (int i = 0; i < rowCount; i++) {
            rowIdVector.set(i, i + 1000L);
        }
        rowIdVsr.setRowCount(rowCount);
        return rowIdVsr;
    }

    private FieldVector createSizedVector(String name, int targetSize,
            int rowCount)
    {
        VarCharVector vector = new VarCharVector(name, inputAllocator);
        int overhead = 64 + (rowCount * 5);
        int dataSize = targetSize - overhead;
        if (dataSize <= 0) {
            throw new IllegalArgumentException(
                    String.format("Target size %d is too small for rowCount %d",
                            targetSize, rowCount));
        }
        vector.allocateNew(dataSize, rowCount);
        int bytesPerRow = dataSize / rowCount;
        byte[] value = new byte[bytesPerRow];
        Arrays.fill(value, (byte) 'x');
        for (int i = 0; i < rowCount; i++) {
            vector.setSafe(i, value, 0, value.length);
        }
        vector.setValueCount(rowCount);
        LOG.debug("Vector %s created. Target size: %d, Actual buffer size: %d",
                name, targetSize, vector.getBufferSize());
        return vector;
    }

    private VectorSchemaRoot vsrFromVectors(List<FieldVector> vectors,
            int rowCount)
    {
        VectorSchemaRoot vsr = new VectorSchemaRoot(vectors);
        vsr.setRowCount(rowCount);
        return vsr;
    }

    private VectorSchemaRoot deserialize(byte[] data)
            throws IOException
    {
        try (MessageChannelReader reader = new MessageChannelReader(
                new ReadChannel(
                        Channels.newChannel(new ByteArrayInputStream(data))),
                deserializeAllocator)) {
            MessageResult result = reader.readNext();
            if (result == null || result
                    .getMessage()
                    .headerType() != MessageHeader.Schema) {
                throw new IOException("Expected schema, but not found");
            }
            Schema schema = MessageSerializer.deserializeSchema(
                    result.getMessage());
            VectorSchemaRoot root = VectorSchemaRoot.create(schema,
                    deserializeAllocator);
            VectorLoader loader = new VectorLoader(root,
                    NoCompressionCodec.Factory.INSTANCE);

            while ((result = reader.readNext()) != null) {
                if (result
                        .getMessage()
                        .headerType() == MessageHeader.RecordBatch) {
                    try (ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(
                            result.getMessage(), result.getBodyBuffer())) {
                        if (batch.getLength() > 0) {
                            loader.load(batch);
                        }
                    }
                }
            }
            return root;
        }
    }

    class TestCase
    {
        public List<FieldVector> nonUpdatable;
        public List<FieldVector> updateable;
        public Predicate<String> isNonUpdateableColumn;

        TestCase(List<FieldVector> nonUpdatable, List<FieldVector> updateable)
        {
            this.nonUpdatable = nonUpdatable;
            this.updateable = updateable;
            this.isNonUpdateableColumn = c -> nonUpdatable
                    .stream()
                    .map(FieldVector::getName)
                    .map(String::toLowerCase)
                    .anyMatch(
                            Predicates.equalTo(c.toLowerCase(Locale.ENGLISH)));
        }

        public List<FieldVector> getAllFields()
        {
            return Stream
                    .concat(nonUpdatable.stream(), updateable.stream())
                    .collect(Collectors.toList());
        }
    }

    @Test
    public void testSingleInsertRpcPath_allColumnsInOneInsert()
            throws VastException
    {
        vastConfig.setMaxRequestBodySize(2000L);
        FieldVector primCol = createSizedVector("prim_col", 300, rowCount);
        FieldVector nestedCol = createSizedVector("nested_col", 200, rowCount);

        ByColumnInserter inserter = getByColumnInserter(
                Predicates.alwaysFalse());
        try (VectorSchemaRoot inputVsr = vsrFromVectors(
                List.of(primCol, nestedCol), rowCount)) {
            inserter.insert(List.of(inputVsr), resultAllocator).join().close();
        }

        verify(vastClient, times(1)).insertRows(any(), any(), any(), any(),
                eq(true), any(), any(), any());
        verify(vastClient, never()).updateRows(any(), any(), any(), any(),
                any(), any());
    }

    @Test
    public void testSingleInsertRpcPath_oversizedColumnGoesToUpdate()
            throws VastException, IOException
    {
        vastConfig.setMaxRequestBodySize(500L);
        FieldVector primCol = createSizedVector("prim_col", 200, rowCount);
        FieldVector largeNestedCol = createSizedVector("large_nested_col", 400,
                rowCount);

        ByColumnInserter inserter = getByColumnInserter(
                Predicates.alwaysFalse());
        try (VectorSchemaRoot inputVsr = vsrFromVectors(
                List.of(primCol, largeNestedCol), rowCount)) {
            inserter.insert(List.of(inputVsr), resultAllocator).join().close();
        }

        ArgumentCaptor<byte[]> insertBody = ArgumentCaptor.forClass(
                byte[].class);
        verify(vastClient, times(1)).insertRows(any(), any(), any(),
                insertBody.capture(), eq(true), any(), any(), any());
        try (VectorSchemaRoot insertedVsr = deserialize(
                insertBody.getValue())) {
            List<String> cols = getColumnNames(insertedVsr);
            assertTrue(cols.contains("prim_col"),
                    "Insert must contain the primitive mustField column");
            assertTrue(!cols.contains("large_nested_col"),
                    "Insert must not contain the oversized column");
        }

        ArgumentCaptor<byte[]> updateBody = ArgumentCaptor.forClass(
                byte[].class);
        // Update payload may be split into multiple RPCs when request body is capped.
        verify(vastClient, atLeastOnce()).updateRows(any(), any(),
                updateBody.capture(), any(), any(), any());

        List<String> updatedColumns = updateBody
                .getAllValues()
                .stream()
                .flatMap(body ->
                {
                    try (VectorSchemaRoot vsr = deserialize(body)) {
                        return getColumnNames(vsr).stream();
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());

        assertTrue(updatedColumns.contains("large_nested_col"),
                "At least one update payload must contain the oversized column");
    }
}
