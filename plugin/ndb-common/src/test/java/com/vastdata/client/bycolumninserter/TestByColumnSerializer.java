/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.bycolumninserter;

import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.metrics.RecordBatchSplitterMetrics;
import com.vastdata.client.rowid.RowIDStrategyType;
import com.vastdata.client.schema.VastPayloadSerializer;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestByColumnSerializer
{
    private static final int ROW_COUNT = 10;

    private RootAllocator allocator;
    private BufferAllocator workAllocator;
    private BufferAllocator deserializeAllocator;
    private VastConfig vastConfig;

    @BeforeMethod
    public void setUp()
    {
        allocator = new RootAllocator();
        workAllocator = allocator.newChildAllocator("work", 0, Long.MAX_VALUE);
        deserializeAllocator = allocator.newChildAllocator("deserialize", 0,
                Long.MAX_VALUE);
        vastConfig = new VastConfig();
    }

    @AfterMethod
    public void tearDown()
    {
        workAllocator.close();
        deserializeAllocator.close();
        allocator.close();
    }

    private static Field intField(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Int(32, true)),
                null);
    }

    private static Field varcharField(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Utf8()), null);
    }

    private static Field fixedBinaryField(String name, int byteWidth)
    {
        return new Field(name,
                FieldType.nullable(new ArrowType.FixedSizeBinary(byteWidth)),
                null);
    }

    private static Field structField(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Struct()),
                List.of(varcharField("inner")));
    }

    private static Field structField(String name, List<Field> children)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Struct()),
                children);
    }

    private void populateInt(VectorSchemaRoot vsr,
                             String fieldName,
                             int rowCount)
    {
        IntVector vec = (IntVector) vsr.getVector(fieldName);
        vec.allocateNew(rowCount);
        for (int i = 0; i < rowCount; i++) {
            vec.setSafe(i, i + 1);
        }
        vec.setValueCount(rowCount);
    }

    private void populateVarChar(VectorSchemaRoot vsr,
                                 String fieldName,
                                 int rowCount,
                                 int bytesPerRow)
    {
        VarCharVector vec = (VarCharVector) vsr.getVector(fieldName);
        vec.allocateNew((long) bytesPerRow * rowCount, rowCount);
        byte[] value = new byte[bytesPerRow];
        Arrays.fill(value, (byte) 'x');
        for (int i = 0; i < rowCount; i++) {
            vec.setSafe(i, value, 0, value.length);
        }
        vec.setValueCount(rowCount);
    }

    private void populateFixedBinary(VectorSchemaRoot vsr,
                                     String fieldName,
                                     int rowCount)
    {
        FixedSizeBinaryVector vec = (FixedSizeBinaryVector) vsr.getVector(
                fieldName);
        vec.allocateNew(rowCount);
        byte[] value = new byte[vec.getByteWidth()];
        Arrays.fill(value, (byte) 'f');
        for (int i = 0; i < rowCount; i++) {
            vec.setSafe(i, value);
        }
        vec.setValueCount(rowCount);
    }

    private void populateStruct(VectorSchemaRoot vsr,
                                String fieldName,
                                int rowCount,
                                int bytesPerRow)
    {
        StructVector structVec = (StructVector) vsr.getVector(fieldName);
        VarCharVector inner = structVec.addOrGet("inner",
                FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);
        inner.allocateNew((long) bytesPerRow * rowCount, rowCount);
        byte[] value = new byte[bytesPerRow];
        Arrays.fill(value, (byte) 'x');
        for (int i = 0; i < rowCount; i++) {
            inner.setSafe(i, value, 0, value.length);
            structVec.setIndexDefined(i);
        }
        inner.setValueCount(rowCount);
        structVec.setValueCount(rowCount);
    }

    private void populateStructChildren(VectorSchemaRoot vsr,
                                        String fieldName,
                                        int rowCount,
                                        int childBytesPerRow)
    {
        StructVector structVec = (StructVector) vsr.getVector(fieldName);
        for (FieldVector child : structVec.getChildrenFromFields()) {
            String childName = child.getName();
            ArrowType type = child.getField().getType();
            if (type instanceof ArrowType.Int) {
                IntVector intChild = structVec.addOrGet(childName,
                        FieldType.nullable(type), IntVector.class);
                intChild.allocateNew(rowCount);
                for (int i = 0; i < rowCount; i++) {
                    intChild.setSafe(i, i + 1);
                }
                intChild.setValueCount(rowCount);
            }
            else if (type instanceof ArrowType.Utf8) {
                VarCharVector vcChild = structVec.addOrGet(childName,
                        FieldType.nullable(type), VarCharVector.class);
                vcChild.allocateNew((long) childBytesPerRow * rowCount,
                        rowCount);
                for (int i = 0; i < rowCount; i++) {
                    byte[] bytes = String.valueOf(i).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    vcChild.setSafe(i, bytes, 0, bytes.length);
                }
                vcChild.setValueCount(rowCount);
            }
        }
        for (int i = 0; i < rowCount; i++) {
            structVec.setIndexDefined(i);
        }
        structVec.setValueCount(rowCount);
    }

    private ByColumnSerializer makeSerializer()
    {
        return new ByColumnSerializer(vastConfig,
                new RecordBatchSplitterMetrics(),
                RowIDStrategyType.UNSIGNED_INT64, "test-trace-serializer");
    }

    private long recordBatchBodySize(VectorSchemaRoot vsr)
    {
        VectorUnloader unloader = new VectorUnloader(vsr);
        try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
            return batch.computeBodyLength();
        }
    }

    private ByColumnSerializer.InsertPlan plan(VectorSchemaRoot vsr,
                                               Predicate<String> isNonUpdatable)
            throws VastUserException
    {
        return makeSerializer().makeInsertPlan(List.of(vsr), isNonUpdatable,
                vastConfig, workAllocator);
    }

    private Set<String> insertColumns(byte[] payload)
            throws IOException
    {
        try (MessageChannelReader reader = new MessageChannelReader(
                new ReadChannel(
                        Channels.newChannel(new ByteArrayInputStream(payload))),
                deserializeAllocator)) {
            MessageResult result = reader.readNext();
            if (result == null || result
                    .getMessage()
                    .headerType() != MessageHeader.Schema) {
                throw new IOException("Expected schema message");
            }
            Schema schema = MessageSerializer.deserializeSchema(
                    result.getMessage());
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema,
                    deserializeAllocator)) {
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
                return root
                        .getSchema()
                        .getFields()
                        .stream()
                        .map(Field::getName)
                        .filter(name -> !name.equals(ROW_ID_FIELD_NAME))
                        .collect(Collectors.toSet());
            }
        }
    }

    private Set<String> updateColumns(List<VectorSchemaRoot> updateVsrs)
    {
        return updateVsrs
                .stream()
                .flatMap(vsr -> vsr.getSchema().getFields().stream())
                .map(Field::getName)
                .filter(name -> !name.equals(ROW_ID_FIELD_NAME))
                .collect(Collectors.toSet());
    }

    @Test
    public void testNonUpdatableAllColumnsFitInInsert()
            throws VastUserException, IOException
    {
        Schema schema = new Schema(
                List.of(intField("id"), intField("age"), varcharField("name")));
        vastConfig.setMaxRequestBodySize(5000L);

        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateInt(vsr, "id", ROW_COUNT);
            populateInt(vsr, "age", ROW_COUNT);
            populateVarChar(vsr, "name", ROW_COUNT, 20);
            vsr.setRowCount(ROW_COUNT);

            ByColumnSerializer.InsertPlan result = plan(vsr,
                    col -> col.equals("id"));

            assertEquals(result.insertPayloads.size(), 1, "one insert payload");
            assertEquals(insertColumns(result.insertPayloads.get(0)),
                    Set.of("id", "age", "name"));
            assertEquals(updateColumns(result.updateVsrs), Set.of());
        }
    }

    @Test
    public void testNonUpdatableColumnSpillsToUpdate()
            throws VastUserException, IOException
    {
        Schema schema = new Schema(
                List.of(intField("id"), varcharField("description")));
        vastConfig.setMaxRequestBodySize(600L);

        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateInt(vsr, "id", ROW_COUNT);
            populateVarChar(vsr, "description", ROW_COUNT,
                    100); // ~1000B data buffer
            vsr.setRowCount(ROW_COUNT);

            ByColumnSerializer.InsertPlan result = plan(vsr,
                    col -> col.equals("id"));

            assertEquals(result.insertPayloads.size(), 1);
            assertEquals(insertColumns(result.insertPayloads.get(0)),
                    Set.of("id"));
            assertEquals(updateColumns(result.updateVsrs),
                    Set.of("description"));
        }
    }

    @Test
    public void testNonUpdatableColumnToBigForInsert()
            throws VastUserException, IOException
    {
        Schema schema = new Schema(
                List.of(intField("id"), intField("small_col"),
                        varcharField("big_col")));
        vastConfig.setMaxRequestBodySize(800L);

        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateInt(vsr, "id", ROW_COUNT);
            populateInt(vsr, "small_col", ROW_COUNT);
            populateVarChar(vsr, "big_col", ROW_COUNT, 100); // ~1000B
            vsr.setRowCount(ROW_COUNT);

            ByColumnSerializer.InsertPlan result = plan(vsr,
                    col -> col.equals("id"));

            assertEquals(insertColumns(result.insertPayloads.get(0)),
                    Set.of("id", "small_col"));
            assertEquals(updateColumns(result.updateVsrs),
                    Set.of("big_col"));
        }
    }

    @Test
    public void testNonUpdatableSplitByRowsWhenExceedsBudget()
            throws VastUserException
    {
        Schema schema = new Schema(
                List.of(varcharField("id"), varcharField("tag")));
        vastConfig.setMaxRequestBodySize(1200L);

        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateVarChar(vsr, "id", ROW_COUNT, 100);  // ~1000B data
            populateVarChar(vsr, "tag", ROW_COUNT, 100);
            vsr.setRowCount(ROW_COUNT);

            ByColumnSerializer.InsertPlan result = plan(vsr,
                    col -> col.equals("id") || col.equals("tag"));

            assertTrue(result.insertPayloads.size() > 1,
                    "oversized non-updatable columns must produce multiple insert payloads via row splitting");
            assertEquals(updateColumns(result.updateVsrs), Set.of());
        }
    }

    @Test
    public void testNonUpdatableNestedColumns()
            throws VastUserException, IOException
    {
        Schema schema = new Schema(
                List.of(intField("id"), structField("metadata")));
        vastConfig.setMaxRequestBodySize(5000L);

        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateInt(vsr, "id", ROW_COUNT);
            populateStruct(vsr, "metadata", ROW_COUNT, 20);
            vsr.setRowCount(ROW_COUNT);

            ByColumnSerializer.InsertPlan result = plan(vsr,
                    col -> col.equals("id"));

            assertEquals(result.insertPayloads.size(), 1);
            assertEquals(insertColumns(result.insertPayloads.get(0)),
                    Set.of("id", "metadata"));
            assertEquals(updateColumns(result.updateVsrs), Set.of());
        }
    }

    @Test
    public void testNonUpdatableNestedNonUpdatable()
            throws VastUserException, IOException
    {
        Schema schema = new Schema(
                List.of(structField("metadata"), intField("score")));
        vastConfig.setMaxRequestBodySize(5000L);

        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateStruct(vsr, "metadata", ROW_COUNT, 20);
            populateInt(vsr, "score", ROW_COUNT);
            vsr.setRowCount(ROW_COUNT);

            ByColumnSerializer.InsertPlan result = plan(vsr,
                    col -> col.equals("metadata"));

            assertEquals(result.insertPayloads.size(), 1);
            assertEquals(insertColumns(result.insertPayloads.get(0)),
                    Set.of("metadata", "score"));
            assertEquals(updateColumns(result.updateVsrs), Set.of());
        }
    }

    @Test
    public void testNonUpdatableNestedWithPrimitive()
            throws VastUserException, IOException
    {
        Schema schema = new Schema(
                List.of(structField("metadata"), varcharField("description")));
        vastConfig.setMaxRequestBodySize(1200L);

        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateStruct(vsr, "metadata", ROW_COUNT,
                    100);      // real IPC ≈ 1400B
            populateVarChar(vsr, "description", ROW_COUNT,
                    100);  // ~1000B buffer
            vsr.setRowCount(ROW_COUNT);

            ByColumnSerializer.InsertPlan result = plan(vsr,
                    col -> col.equals("metadata"));

            assertEquals(insertColumns(result.insertPayloads.get(0)),
                    Set.of("metadata"));
            assertEquals(updateColumns(result.updateVsrs),
                    Set.of("description"));
        }
    }

    @Test
    public void testSingleInsertRpcPrimitiveColumnsFit()
            throws VastUserException, IOException
    {
        Schema schema = new Schema(List.of(intField("col_a"), intField("col_b"),
                varcharField("col_c")));
        vastConfig.setMaxRequestBodySize(5000L);

        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateInt(vsr, "col_a", ROW_COUNT);
            populateInt(vsr, "col_b", ROW_COUNT);
            populateVarChar(vsr, "col_c", ROW_COUNT, 20);
            vsr.setRowCount(ROW_COUNT);

            ByColumnSerializer.InsertPlan result = plan(vsr, col -> false);

            assertEquals(result.insertPayloads.size(), 1, "one insert payload");
            assertEquals(insertColumns(result.insertPayloads.get(0)),
                    Set.of("col_a", "col_b", "col_c"));
            assertEquals(updateColumns(result.updateVsrs), Set.of());
        }
    }

    @Test
    public void testSingleInsertRpcLargeColumnSpillsToUpdate()
            throws VastUserException, IOException
    {
        Schema schema = new Schema(
                List.of(intField("col_a"), varcharField("col_b")));
        vastConfig.setMaxRequestBodySize(500L);

        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateInt(vsr, "col_a", ROW_COUNT);
            populateVarChar(vsr, "col_b", ROW_COUNT, 100); // ~1000B buffer
            vsr.setRowCount(ROW_COUNT);

            ByColumnSerializer.InsertPlan result = plan(vsr, col -> false);

            assertEquals(result.insertPayloads.size(), 1);
            assertEquals(insertColumns(result.insertPayloads.get(0)),
                    Set.of("col_a"));
            assertEquals(updateColumns(result.updateVsrs),
                    Set.of("col_b"));
        }
    }

    @Test
    public void testSingleInsertRpcNestedColumnAlwaysGoesToUpdate()
            throws VastUserException, IOException
    {
        // Nested columns should get inserted when they fit
        Schema schema = new Schema(
                List.of(intField("prim_col"), structField("nested_col")));
        vastConfig.setMaxRequestBodySize(5000L);

        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateInt(vsr, "prim_col", ROW_COUNT);
            populateStruct(vsr, "nested_col", ROW_COUNT, 20);
            vsr.setRowCount(ROW_COUNT);

            ByColumnSerializer.InsertPlan result = plan(vsr, col -> false);

            assertEquals(insertColumns(result.insertPayloads.get(0)),
                    Set.of("prim_col", "nested_col"));
            assertEquals(updateColumns(result.updateVsrs), Set.of());
        }
    }

    @Test
    public void testSingleInsertRpcMustColumnSplitByRows()
            throws VastUserException
    {
        Schema schema = new Schema(List.of(varcharField("heavy_col")));
        vastConfig.setMaxRequestBodySize(600L);

        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateVarChar(vsr, "heavy_col", ROW_COUNT,
                    100); // ~1000B for 10 rows
            vsr.setRowCount(ROW_COUNT);

            ByColumnSerializer.InsertPlan result = plan(vsr, col -> false);

            assertTrue(result.insertPayloads.size() > 1,
                    "oversized must column must be split into multiple payloads by rows");
        }
    }

    @Test
    public void testSimpleInsertSingleNestedColumn()
            throws VastUserException, IOException
    {
        Schema schema = new Schema(List.of(structField("nested1")));
        vastConfig.setMaxRequestBodySize(5000L);

        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateStruct(vsr, "nested1", ROW_COUNT, 20);
            vsr.setRowCount(ROW_COUNT);

            ByColumnSerializer.InsertPlan result = plan(vsr, col -> false);

            assertEquals(result.insertPayloads.size(), 1);
            assertEquals(insertColumns(result.insertPayloads.get(0)),
                    Set.of("nested1"));
            assertEquals(updateColumns(result.updateVsrs), Set.of());
        }
    }

    @Test
    public void testSimpleInsertFirstInInsertRestAlwaysInUpdate()
            throws VastUserException, IOException
    {
        //todo - is this the correct behavior because the second nested column can fit in the insert payload?
        Schema schema = new Schema(
                List.of(structField("nested1"), structField("nested2")));
        vastConfig.setMaxRequestBodySize(5000L);

        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateStruct(vsr, "nested1", ROW_COUNT, 20);
            populateStruct(vsr, "nested2", ROW_COUNT, 20);
            vsr.setRowCount(ROW_COUNT);

            ByColumnSerializer.InsertPlan result = plan(vsr, col -> false);

            assertEquals(result.insertPayloads.size(), 1);
            assertEquals(insertColumns(result.insertPayloads.get(0)),
                    Set.of("nested1", "nested2"));
            assertEquals(updateColumns(result.updateVsrs), Set.of());
        }
    }

    @Test
    public void testSimpleInsertMultipleNestedColumns()
            throws VastUserException, IOException
    {
        //todo - is this the correct behavior because the second and third nested column can fit in the insert payload?
        Schema schema = new Schema(
                List.of(structField("nested1"), structField("nested2"),
                        structField("nested3")));
        vastConfig.setMaxRequestBodySize(5000L);

        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateStruct(vsr, "nested1", ROW_COUNT, 20);
            populateStruct(vsr, "nested2", ROW_COUNT, 20);
            populateStruct(vsr, "nested3", ROW_COUNT, 20);
            vsr.setRowCount(ROW_COUNT);

            ByColumnSerializer.InsertPlan result = plan(vsr, col -> false);

            assertEquals(insertColumns(result.insertPayloads.get(0)),
                    Set.of("nested1", "nested2", "nested3"));
            assertEquals(updateColumns(result.updateVsrs), Set.of());
        }
    }

    @Test
    public void testSimpleInsertColumnSplitByRows()
            throws VastUserException
    {
        Schema schema = new Schema(
                List.of(structField("nested1"), structField("nested2")));
        vastConfig.setMaxRequestBodySize(2000L);

        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateStruct(vsr, "nested1", ROW_COUNT,
                    200); // ~2400B for 10 rows > 2000B
            populateStruct(vsr, "nested2", ROW_COUNT, 200);
            vsr.setRowCount(ROW_COUNT);

            ByColumnSerializer.InsertPlan result = plan(vsr, col -> false);

            assertTrue(result.insertPayloads.size() > 1,
                    "oversized must column must be split into multiple insert payloads");
            assertEquals(updateColumns(result.updateVsrs),
                    Set.of("nested2"));
        }
    }

    @Test
    public void testFieldVectorEstimatedSizePrimitiveIntVector()
    {
        Schema schema = new Schema(List.of(intField("x")));
        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateInt(vsr, "x", 10000);
            IntVector vec = (IntVector) vsr.getVector("x");
            int estimatedSize = SerializedSizeApproximator.fieldVectorEstimatedSize(
                    vec);

            int actual = VastPayloadSerializer
                    .getInstanceForRecordBatch()
                    .apply(vsr)
                    .get().length;

            System.out.println(String.format(
                    "Estimated size: %d, actual serialized size: %d",
                    estimatedSize, actual));

            assertTrue(estimatedSize >= 40,
                    "estimated size must be larger or equal to actual data buffer (40B for 10 rows)");
            assertTrue(estimatedSize >= actual, String.format(
                    "primitive vector: estimated size %d should be greater then serialize %d",
                    estimatedSize, actual));
        }
    }

    @Test
    public void testFieldVectorEstimatedSizePrimitiveFixedBinaryVector()
    {
        Schema schema = new Schema(List.of(fixedBinaryField("text", 100)));
        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            int rowCount = 100000;
            populateFixedBinary(vsr, "text", rowCount);
            vsr.setRowCount(rowCount);

            FixedSizeBinaryVector vec = (FixedSizeBinaryVector) vsr.getVector(
                    "text");
            int estimatedSize = SerializedSizeApproximator.fieldVectorEstimatedSize(
                    vec);

            int actual = VastPayloadSerializer
                    .getInstanceForRecordBatch()
                    .apply(vsr)
                    .get().length;

            System.out.println(String.format(
                    "Estimated size: %d, actual serialized size: %d",
                    estimatedSize, actual));

            assertTrue(estimatedSize > 500, String.format(
                    "FixedBinary estimated size should be > 500, got %d",
                    estimatedSize));
            assertTrue(estimatedSize >= actual, String.format(
                    "primitive vector: estimated size %d should be >= actual serialized size %d",
                    estimatedSize, actual));
        }
    }

    @Test
    public void testFieldVectorEstimatedSizeNestedStructVectorIncludesChildren()
    {
        Schema schema = new Schema(List.of(structField("s")));
        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            int rowCount = 10000;
            populateStruct(vsr, "s", rowCount, 100);
            vsr.setRowCount(rowCount);

            StructVector structVec = (StructVector) vsr.getVector("s");
            int estimatedSize = SerializedSizeApproximator.fieldVectorEstimatedSize(
                    structVec);

            int structOwnBufferSize = structVec.getBufferSize();
            VarCharVector child = (VarCharVector) structVec
                    .getChildrenFromFields()
                    .get(0);
            int actual = VastPayloadSerializer
                    .getInstanceForRecordBatch()
                    .apply(vsr)
                    .get().length;
            int expectedRecursiveSize = structOwnBufferSize;

            assertTrue(estimatedSize >= actual, String.format(
                    "nested vector estimated size (%d) must include children buffers. expected total>=%d",
                    estimatedSize, actual));
        }
    }

    @Test
    public void testFieldVectorEstimatedSizeNestedStruct()
    {
        Schema schema = new Schema(List.of(structField("s")));
        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateStruct(vsr, "s", ROW_COUNT, 200);
            vsr.setRowCount(ROW_COUNT);

            StructVector structVec = (StructVector) vsr.getVector("s");
            int estimatedSize = SerializedSizeApproximator.fieldVectorEstimatedSize(
                    structVec);

            assertTrue(estimatedSize > 2000, String.format(
                    "nested vector with 2000B of child data should have estimated size > 2000, got %d",
                    estimatedSize));
        }
    }

    @Test
    public void testFieldsVectorEstimatedMultipleFields()
    {
        Schema schema = new Schema(
                List.of(intField("x"), varcharField("t"), structField("s")));
        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            int rowCount = 10000;
            populateInt(vsr, "x", rowCount);
            populateVarChar(vsr, "t", rowCount, 50);
            populateStruct(vsr, "s", rowCount, 100);
            vsr.setRowCount(rowCount);

            FieldVector intVec = vsr.getVector("x");
            FieldVector varCharVec = vsr.getVector("t");
            FieldVector structVec = vsr.getVector("s");

            Map<String, Integer> estimatedSize = SerializedSizeApproximator.approximateSizeByColumnFieldVectors(
                    List.of(intVec, varCharVec, structVec));
            int totalSizeVsr = estimatedSize
                    .values()
                    .stream()
                    .mapToInt(Integer::intValue)
                    .sum();
            int actual = VastPayloadSerializer
                    .getInstanceForRecordBatch()
                    .apply(vsr)
                    .get().length;
            assertTrue(actual <= totalSizeVsr, String.format(
                    "total vectors sizes should be estimated %d <= actual %d",
                    totalSizeVsr, actual));
        }
    }

    @Test
    public void testEstimateReturnPositiveValue()
    {
        Schema schema = new Schema(List.of(structField("s", List.of())));
        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            populateStructChildren(vsr, "s", 1000, 50);
            vsr.setRowCount(ROW_COUNT);

            StructVector structVec = (StructVector) vsr.getVector("s");
            int estimated = SerializedSizeApproximator.fieldVectorEstimatedSize(
                    structVec);
            assertTrue(estimated > 0, "estimated must be positive");
        }
    }

    @Test
    public void testSerializedStructWithDoubleInnerTypes()
    {
        List<Field> children = List.of(intField("a"), varcharField("b"),
                intField("c"));
        Schema schema = new Schema(List.of(structField("s",
                List.of(structField("a", children), varcharField("b"),
                        intField("c")))));
        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            int rowCount = 100000;
            populateStructChildren(vsr, "s", rowCount, 50);
            vsr.setRowCount(rowCount);

            StructVector structVec = (StructVector) vsr.getVector("s");
            int estimated = SerializedSizeApproximator.fieldVectorEstimatedSize(
                    structVec);
            int actual = VastPayloadSerializer
                    .getInstanceForRecordBatch()
                    .apply(vsr)
                    .get().length;

            assertTrue(estimated > 0, "estimated must be positive");
            assertTrue(actual <= estimated, String.format(
                    "actual serialized (%d) should be <= estimated (%d)",
                    actual, estimated));
        }
    }

    @Test
    public void testSerializedStructWithLargeVarCharChildren()
    {
        Schema schema = new Schema(List.of(structField("s",
                List.of(varcharField("big1"), varcharField("big2")))));
        int scemasize = schema.serializeAsMessage().length;
        System.out.println("schema size: " + scemasize);
        try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema,
                allocator)) {
            int rowCount = 10000;
            populateStructChildren(vsr, "s", rowCount, 200);
            vsr.setRowCount(rowCount);

            StructVector structVec = (StructVector) vsr.getVector("s");
            int estimated = SerializedSizeApproximator.fieldVectorEstimatedSize(
                    structVec);
            int actual = VastPayloadSerializer
                    .getInstanceForRecordBatch()
                    .apply(vsr)
                    .get().length;

            assertTrue(estimated > 4000, String.format(
                    "struct with 2x200B varchar children (10 rows) should estimate > 4000, got %d",
                    estimated));
            assertTrue(actual <= estimated, String.format(
                    "actual serialized (%d) should be <= estimated (%d)",
                    actual, estimated));
        }
    }
}
