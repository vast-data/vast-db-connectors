/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver.handle;

import com.google.common.primitives.Ints;
import com.google.flatbuffers.FlatBufferBuilder;
import com.vastdata.client.schema.ArrowSchemaUtils;
import com.vastdata.client.schema.VastArrowPayloadSerializer;
import com.vastdata.mockserver.MockMapSchema;
import com.vastdata.mockserver.MockTable;
import com.vastdata.mockserver.MockView;
import io.airlift.log.Logger;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import vast_flatbuf.tabular.CreateViewRequest;
import vast_flatbuf.tabular.ListSchemasResponse;
import vast_flatbuf.tabular.ListTablesResponse;
import vast_flatbuf.tabular.ListViewsResponse;
import vast_flatbuf.tabular.ObjectDetails;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_DEC128_FIELD;

public final class MockSchemaUtil
{
    private static final Logger LOG = Logger.get(MockSchemaUtil.class);
    private static final RootAllocator allocator = new RootAllocator();
    private static final VastArrowPayloadSerializer<Schema> schemaSerializer = VastArrowPayloadSerializer.getInstanceForSchema();

    private MockSchemaUtil()
    {
    }

    static FlatBufferBuilder getListSchemasReply(String bucket,
            Set<MockMapSchema> mockMapSchemas)
    {
        FlatBufferBuilder flatbuffBuilder = new FlatBufferBuilder();
        int bucketOffset = flatbuffBuilder.createString(bucket);
        int[] schemaDetailsOffsets = new int[mockMapSchemas.size()];
        int i = 0;
        for (MockMapSchema mockSchema : mockMapSchemas) {
            String name = mockSchema.getName();
            LOG.info("Adding schema name to ListSchemasResponse: %s", name);
            int schemaNameOffset = flatbuffBuilder.createString(name);
            int propertiesOffset = flatbuffBuilder.createString("");
            int handleOffset = flatbuffBuilder.createString("0");
            int objectDetails = ObjectDetails.createObjectDetails(
                    flatbuffBuilder, schemaNameOffset, propertiesOffset,
                    handleOffset, 0L, 0L, 0L, false, 0L, 0L, 0L, false, 281474976710655L, 0);
            schemaDetailsOffsets[i] = objectDetails;
            i++;
        }
        int schemasVectorOffset = flatbuffBuilder.createVectorOfTables(
                schemaDetailsOffsets);
        int finalOffset = ListSchemasResponse.createListSchemasResponse(
                flatbuffBuilder, bucketOffset, schemasVectorOffset);
        flatbuffBuilder.finish(finalOffset);
        return flatbuffBuilder;
    }

    public static FlatBufferBuilder getListViewsReply(String bucket,
            String schema, MockMapSchema mockMapSchema, String exactMatch)
    {
        FlatBufferBuilder flatbuffBuilder = new FlatBufferBuilder();
        int bucketOffset = flatbuffBuilder.createString(bucket);
        int schemaNameOffset = flatbuffBuilder.createString(schema);
        Map<String, MockView> views = mockMapSchema.getViews();
        ArrayList<Integer> viewsDetailsOffset = new ArrayList<>();
        for (String name : views.keySet()) {
            if (exactMatch == null) {
                LOG.info("Adding table name to ListViewsResponse: %s", name);
            }
            else {
                if (exactMatch.equals(name)) {
                    LOG.info(
                            "Adding table name to ListViewsResponse: %s matching filter: %s",
                            name, exactMatch);
                }
                else {
                    LOG.info(
                            "Filtered out table name to ListViewsResponse: %s not matching filter: %s",
                            name, exactMatch);
                    continue;
                }
            }
            setDetailsAndOffset(name, flatbuffBuilder, viewsDetailsOffset);
        }
        int tablesVectorOffset = flatbuffBuilder.createVectorOfTables(
                Ints.toArray(viewsDetailsOffset));
        int finalOffset = ListViewsResponse.createListViewsResponse(
                flatbuffBuilder, bucketOffset, schemaNameOffset,
                tablesVectorOffset);
        flatbuffBuilder.finish(finalOffset);
        return flatbuffBuilder;
    }

    public static FlatBufferBuilder getListTablesReply(String bucket,
            String schema, MockMapSchema mockMapSchema, String exactMatch)
    {
        FlatBufferBuilder flatbuffBuilder = new FlatBufferBuilder();
        int bucketOffset = flatbuffBuilder.createString(bucket);
        int schemaNameOffset = flatbuffBuilder.createString(schema);
        Map<String, MockTable> tables = mockMapSchema.getTables();
        ArrayList<Integer> tableDetailsOffset = new ArrayList<>();
        for (String tableName : tables.keySet()) {
            if (exactMatch == null) {
                LOG.info("Adding table name to ListTablesResponse: %s",
                        tableName);
            }
            else {
                if (exactMatch.equals(tableName)) {
                    LOG.info(
                            "Adding table name to ListTablesResponse: %s matching filter: %s",
                            tableName, exactMatch);
                }
                else {
                    LOG.info(
                            "Filtered out table name to ListTablesResponse: %s not matching filter: %s",
                            tableName, exactMatch);
                    continue;
                }
            }
            setDetailsAndOffset(tableName, flatbuffBuilder, tableDetailsOffset);
        }
        int tablesVectorOffset = flatbuffBuilder.createVectorOfTables(
                Ints.toArray(tableDetailsOffset));
        int finalOffset = ListTablesResponse.createListTablesResponse(
                flatbuffBuilder, bucketOffset, schemaNameOffset,
                tablesVectorOffset);
        flatbuffBuilder.finish(finalOffset);
        return flatbuffBuilder;
    }

    private static void setDetailsAndOffset(String tableName,
            FlatBufferBuilder flatbuffBuilder,
            ArrayList<Integer> tableDetailsOffset)
    {
        int tableNameOffset = flatbuffBuilder.createString(tableName);
        int propertiesOffset = flatbuffBuilder.createString("");
        int handleOffset = flatbuffBuilder.createString("0");
        int objectDetails = ObjectDetails.createObjectDetails(flatbuffBuilder,
                tableNameOffset, propertiesOffset, handleOffset, 0L, 0L, 0L, false,
                0L, 0L, 0L, false, 281474976710655L, 0);
        tableDetailsOffset.add(objectDetails);
    }

    public static Schema parseTableSchema(byte[] schemaBytes)
            throws IOException
    {
        return new ArrowSchemaUtils().parseSchema(schemaBytes, allocator);
    }

    public static List<Field> parseTableFields(byte[] schemaBytes)
            throws IOException
    {
        return parseTableSchema(schemaBytes).getFields();
    }

    public static Optional<byte[]> serializeFields(Collection<Field> fields)
    {
        return schemaSerializer.apply(new Schema(fields), allocator);
    }

    public static ParsedViewData deserializeCreateViewRequestBody(byte[] bytes)
            throws IOException
    {
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        CreateViewRequest req = CreateViewRequest.getRootAsCreateViewRequest(
                wrap);
        int schemaLength = req.viewDataArrowSchemaLength();
        int mdLength = req.viewMetadataArrowBufferLength();

        byte[] newSchemArr = new byte[schemaLength];
        byte[] newMDArr = new byte[mdLength];
        req.viewDataArrowSchemaAsByteBuffer().get(newSchemArr);
        req.viewMetadataArrowBufferAsByteBuffer().get(newMDArr);

        InputStream input = new ByteArrayInputStream(newSchemArr);
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        MessageChannelReader messageChannelReader = new MessageChannelReader(
                new ReadChannel(Channels.newChannel(input)), allocator);

        MessageResult result = messageChannelReader.readNext();
        if (result == null) {
            result = messageChannelReader.readNext();
        }
        Message message = result.getMessage();
        byte headerType = message.headerType();
        Schema viewDataSchema;
        if (headerType == MessageHeader.Schema) {
            viewDataSchema = MessageSerializer.deserializeSchema(message);
        }
        else {
            throw new IOException(
                    "Unexpected header type. Expected Schema but got: " + headerType);
        }
        input = new ByteArrayInputStream(newMDArr);
        messageChannelReader = new MessageChannelReader(
                new ReadChannel(Channels.newChannel(input)), allocator);
        result = messageChannelReader.readNext();
        message = result.getMessage();
        headerType = message.headerType();
        Schema viewDetailsSchema;
        if (headerType == MessageHeader.Schema) {
            viewDetailsSchema = MessageSerializer.deserializeSchema(message);
        }
        else {
            throw new IOException(
                    "Unexpected header type. Expected Schema but got: " + headerType);
        }
        result = messageChannelReader.readNext();
        message = result.getMessage();
        headerType = message.headerType();
        if (headerType == MessageHeader.RecordBatch) {
            ArrowBuf bodyBuffer = result.getBodyBuffer();

            // For zero-length batches, need an empty buffer to deserialize the batch
            if (bodyBuffer == null) {
                bodyBuffer = allocator.getEmpty();
            }

            VectorSchemaRoot root = VectorSchemaRoot.create(viewDetailsSchema,
                    allocator);
            VectorLoader loader = new VectorLoader(root,
                    NoCompressionCodec.Factory.INSTANCE);
            try (ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(
                    message, bodyBuffer)) {
                loader.load(batch); // load `root` vectors from batch
            }
            return new ParsedViewData(viewDataSchema, root);
        }
        else {
            throw new IOException(
                    "Unexpected header type. Expected RecordBatch but got: " + headerType);
        }
    }

    public static class ParsedViewData
    {
        private final Schema viewDataSchema;
        private final VectorSchemaRoot viewDetails;

        public ParsedViewData(Schema viewDataSchema,
                VectorSchemaRoot viewDetails)
        {
            this.viewDataSchema = viewDataSchema;
            this.viewDetails = viewDetails;
        }

        public Schema getViewDataSchema()
        {
            return viewDataSchema;
        }

        public VectorSchemaRoot getViewDetails()
        {
            return viewDetails;
        }
    }

    static int parseRowCountFromArrowIpc(byte[] body)
    {
        if (body == null || body.length == 0) {
            return 0;
        }
        try (BufferAllocator alloc = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(
                        new ByteArrayInputStream(body), alloc)) {
            if (reader.loadNextBatch()) {
                return reader.getVectorSchemaRoot().getRowCount();
            }
            LOG.warn("No record batch in insert request body, assuming 1 row");
            return 1;
        }
        catch (IOException e) {
            LOG.warn(e, "Failed to parse insert request body, assuming 1 row");
            return 1;
        }
    }

    static byte[] buildRowIdsArrowIpc(int rowCount, Field rowIdField)
            throws IOException
    {
        BiConsumer<BaseFixedWidthVector, Integer> valueSetter;
        if (rowIdField.equals(ROW_ID_DEC128_FIELD)) {
            valueSetter = (vector, i) -> ((DecimalVector) vector).set(i,
                    BigDecimal.valueOf(i));
        }
        else {
            valueSetter = (vector, i) -> ((UInt8Vector) vector).set(i, i);
        }

        try (BufferAllocator alloc = new RootAllocator()) {
            Schema rowIdSchema = new Schema(List.of(rowIdField));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(rowIdSchema,
                    alloc);
                    ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                BaseFixedWidthVector vector = (BaseFixedWidthVector) root.getVector(
                        0);
                vector.allocateNew(rowCount);
                for (int i = 0; i < rowCount; i++) {
                    valueSetter.accept(vector, i);
                }
                vector.setValueCount(rowCount);
                root.setRowCount(rowCount);

                try (ArrowStreamWriter writer = new ArrowStreamWriter(root,
                        null, baos)) {
                    writer.start();
                    writer.writeBatch();
                }
                return baos.toByteArray();
            }
        }
    }
}
