/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import com.google.common.collect.ImmutableList;
import com.google.flatbuffers.FlatBufferBuilder;
import com.vastdata.client.FlatBufferSerializer;
import com.vastdata.client.error.VastExceptionFactory;
import com.vastdata.client.error.VastSerializationException;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import vast_flatbuf.tabular.AlterColumnRequest;
import vast_flatbuf.tabular.AlterSchemaRequest;
import vast_flatbuf.tabular.AlterTableRequest;
import vast_flatbuf.tabular.CreateSchemaRequest;
import vast_flatbuf.tabular.CreateViewRequest;
import vast_flatbuf.tabular.ImportDataRequest;
import vast_flatbuf.tabular.S3File;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;

public class ArrowSchemaUtils
{
    public static final String ROW_ID_FIELD_NAME = "$row_id";
    public static final String VASTDB_EXTERNAL_ROW_ID_COLUMN_NAME = "vastdb_rowid";
    
    // Returned by VAST server for DELETE/UPDATE support (see https://trino.io/docs/current/develop/delete-and-update.html)
    public static final Field ROW_ID_UINT64_FIELD = Field.nullable(ROW_ID_FIELD_NAME, new ArrowType.Int(64, false));
    public static final Field ROW_ID_INT64_FIELD = Field.nullable(ROW_ID_FIELD_NAME, new ArrowType.Int(64, true));
    public static final Field ROW_ID_DEC128_FIELD = Field.nullable(ROW_ID_FIELD_NAME, new ArrowType.Decimal(38, 0 , 128));
    // "vastdb_rowid" is part of https://vastdata.atlassian.net/browse/ORION-132013
    // This feature exposes vast’s internal row ID for user defined allocation and efficient queries
    public static final Field VASTDB_ROW_ID_FIELD = Field.nullable(VASTDB_EXTERNAL_ROW_ID_COLUMN_NAME, new ArrowType.Int(64, true));

    public Schema parseSchema(byte[] buffer, RootAllocator allocator)
            throws IOException
    {
        InputStream stream = new ByteArrayInputStream(buffer);
        try (ArrowStreamReader streamReader = new ArrowStreamReader(stream, allocator)) {
            VectorSchemaRoot root = streamReader.getVectorSchemaRoot();
            return root.getSchema();
        }
    }

    public Schema fromCreateTableContext(CreateTableContext ctx)
    {
        return new Schema(ctx.getFields());
    }

    public byte[] fromAlterTableContext(AlterTableContext ctx)
    {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        Optional<Integer> propsOffset = Optional.empty();
        AlterTableRequest.startAlterTableRequest(builder);
        propsOffset.ifPresent(offset -> AlterTableRequest.addProperties(builder, offset));
        int finishOffset = AlterTableRequest.endAlterTableRequest(builder);
        builder.finish(finishOffset);
        return builder.sizedByteArray();
    }

    public byte[] fromAlterColumnContext(AlterColumnContext ctx)
    {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        Optional<Integer> propsOffset = Optional.empty();
        Optional<Integer> statsOffset = Optional.empty();
        Optional<Map<String, String>> properties = ctx.getProperties();
        if (properties.isPresent()) {
            Optional<byte[]> serializedProperties = VastPayloadSerializer.getInstanceForMap().apply(properties.get());
            if (serializedProperties.isPresent()) {
                propsOffset = Optional.of(builder.createString(ByteBuffer.wrap(serializedProperties.get())));
            }
        }
        Optional<String> serializedStats = ctx.getSerializedStats();
        if (serializedStats.isPresent()) {
            statsOffset = Optional.of(builder.createString(serializedStats.get()));
        }
        AlterColumnRequest.startAlterColumnRequest(builder);
        propsOffset.ifPresent(offset -> AlterColumnRequest.addProperties(builder, offset));
        statsOffset.ifPresent(offset -> AlterColumnRequest.addStats(builder, offset));
        int finishOffset = AlterColumnRequest.endAlterColumnRequest(builder);
        builder.finish(finishOffset);
        return builder.sizedByteArray();
    }

    public byte[] serializeAlterSchemaBody(AlterSchemaContext ctx)
    {
        FlatBufferBuilder builder = new FlatBufferBuilder(128);
        Optional<Integer> propsOffset = Optional.empty();
        if (ctx.getProperties().isPresent()) {
            Map<String, Optional<Object>> stringOptionalMap = ctx.getProperties().get();
            Optional<byte[]> serializedMap = VastPayloadSerializer.getInstanceForMap().apply(stringOptionalMap);
            if (serializedMap.isPresent()) {
                propsOffset = Optional.of(builder.createString(new String(serializedMap.get(), StandardCharsets.UTF_8)));
            }
        }
        AlterSchemaRequest.startAlterSchemaRequest(builder);
        propsOffset.ifPresent(offset -> AlterSchemaRequest.addProperties(builder, offset));
        int finishOffset = AlterSchemaRequest.endAlterSchemaRequest(builder);
        builder.finish(finishOffset);
        return builder.sizedByteArray();
    }

    public byte[] serializeCreateSchemaBody(String properties)
    {
        FlatBufferBuilder builder = new FlatBufferBuilder(128);
        builder.finish(CreateSchemaRequest.createCreateSchemaRequest(
                builder,
                builder.createString(properties)));
        return builder.sizedByteArray(); // TODO: don't copy the data
    }

    public Schema fromChangeColumnLifeCycleContext(TableColumnLifecycleContext ctx)
    {
        return new Schema(ImmutableList.of(ctx.getField()));
    }

    public ByteBuffer newImportDataRequest(ImportDataContext ctx, RootAllocator allocator)
    {
        FlatBufferBuilder b = new FlatBufferBuilder();
        int parquet = b.createString("parquet");
        List<ImportDataFile> sourceFiles = ctx.getSourceFiles();
        List<Integer> offsets = sourceFiles.stream().map(f -> {
            int srcBucket = b.createString(f.getSrcBucket());
            int srcFile = b.createString(f.getSrcFile());
            int partitionOffset;
            partitionOffset = addPartitions(allocator, b, f);
            S3File.startS3File(b);
            S3File.addFormat(b, parquet);
            S3File.addBucketName(b, srcBucket);
            S3File.addFileName(b, srcFile);
            if (partitionOffset > 0) {
                S3File.addPartitions(b, partitionOffset);
            }
            return S3File.endS3File(b);
        }).collect(Collectors.toList());

        ImportDataRequest.startS3FilesVector(b, offsets.size());
        offsets.forEach(b::addOffset);
        int endVector = b.endVector();
        ImportDataRequest.startImportDataRequest(b);
        ImportDataRequest.addS3Files(b, endVector);
        b.finish(ImportDataRequest.endImportDataRequest(b));
        return b.dataBuffer();
    }

    private int addPartitions(RootAllocator allocator, FlatBufferBuilder bufferBuilder, ImportDataFile importDataFile)
    {
        int partitionOffset;
        if (importDataFile.hasSchemaRoot()) {
            Optional<VectorSchemaRoot> vectorSchemaRoot = importDataFile.getVectorSchemaRoot();
            if (!vectorSchemaRoot.isPresent() || vectorSchemaRoot.get().getSchema().getFields().isEmpty()) {
                partitionOffset = 0;
            }
            else {
                partitionOffset = addPartitionsFromVectorSchemaRoot(bufferBuilder, vectorSchemaRoot.get());
            }
        }
        else {
            Map<Field, String> fieldValues = importDataFile.getFieldValuesMap();
            try {
                partitionOffset = (fieldValues == null || fieldValues.isEmpty()) ? 0 : addPartitionsFromDefaultValuesMap(bufferBuilder, fieldValues, allocator);
            }
            catch (VastSerializationException e) {
                throw toRuntime(e);
            }
        }
        return partitionOffset;
    }

    private int addPartitionsFromDefaultValuesMap(FlatBufferBuilder builder, Map<Field, String> values, RootAllocator allocator)
            throws VastSerializationException
    {
        Schema schema = new Schema(values.keySet());
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            IntStream.range(0, schema.getFields().size()).forEach(index -> {
                FieldVector vector = root.getVector(index);
                Field field = vector.getField();
                String val = values.get(field);
                ArrowType arrowType = field.getType();
                switch (arrowType.getTypeID()) {
                    case Int: {
                        ArrowType.Int type = (ArrowType.Int) arrowType;
                        if (type.getIsSigned()) {
                            switch (type.getBitWidth()) {
                                case 8:
                                    TinyIntVector tinyIntVector = (TinyIntVector) vector;
                                    tinyIntVector.allocateNew(1);
                                    tinyIntVector.set(0, Byte.parseByte(val));
                                    break;
                                case 16:
                                    SmallIntVector smallIntVector = (SmallIntVector) vector;
                                    smallIntVector.allocateNew(1);
                                    smallIntVector.set(0, Short.parseShort(val));
                                    break;
                                case 32:
                                    IntVector intVector = (IntVector) vector;
                                    intVector.allocateNew(1);
                                    intVector.set(0, Integer.parseInt(val));
                                    break;
                                case 64:
                                    BigIntVector bigIntVector = (BigIntVector) vector;
                                    bigIntVector.allocateNew(1);
                                    bigIntVector.set(0, Long.parseLong(val));
                                    break;
                            }
                        }
                        else {
                            throw new UnsupportedOperationException("Unsupported unsigned Arrow type: " + arrowType);
                        }
                        break;
                    }
                    case FloatingPoint: {
                        ArrowType.FloatingPoint type = (ArrowType.FloatingPoint) arrowType;
                        switch (type.getPrecision()) {
                            case SINGLE:
                                Float4Vector vector1 = (Float4Vector) vector;
                                vector1.allocateNew(1);
                                vector1.set(0, Float.parseFloat(val));
                                return;
                            case DOUBLE:
                                Float8Vector vector2 = (Float8Vector) vector;
                                vector2.allocateNew(1);
                                vector2.set(0, Double.parseDouble(val));
                                return;
                            default:
                                throw new UnsupportedOperationException("Unsupported floating-point precision: " + type);
                        }
                    }
                    case Utf8: {
                        VarCharVector v = (VarCharVector) vector;
                        v.allocateNew(val.length());
                        v.set(0, val.getBytes(StandardCharsets.UTF_8));
                        break;
                    }
                    case Bool: {
                        BitVector vector1 = (BitVector) vector;
                        vector1.allocateNew(1);
                        vector1.set(0, Boolean.parseBoolean(val) ? 1 : 0);
                        break;
                    }
                    default:
                        throw new UnsupportedOperationException("Unsupported Arrow type: " + arrowType);
                }
            });
            root.setRowCount(1);
            return addPartitionsFromVectorSchemaRoot(builder, root);
        }
    }

    private int addPartitionsFromVectorSchemaRoot(FlatBufferBuilder builder, VectorSchemaRoot root)
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, outputStream)) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        catch (IOException e) {
            throw VastExceptionFactory.serializationException("Failed serializing RecordBatch", e);
        }
        byte[] bytes = outputStream.toByteArray();
        return S3File.createPartitionsVector(builder, bytes);
    }

    public byte[] serializeQueryDataRequestBody(
            String tablePath, Schema schema, FlatBufferSerializer projections, FlatBufferSerializer predicate)
    {
        FlatBufferBuilder builder = new FlatBufferBuilder(128);
        int nameOffset = builder.createString(tablePath);
        int schemaOffset = schema.getSchema(builder); // serialize the schema
        int filterOffset = predicate.serialize(builder);

        org.apache.arrow.computeir.flatbuf.Source.startSource(builder);
        org.apache.arrow.computeir.flatbuf.Source.addName(builder, nameOffset);
        org.apache.arrow.computeir.flatbuf.Source.addSchema(builder, schemaOffset);
        org.apache.arrow.computeir.flatbuf.Source.addFilter(builder, filterOffset);
        int sourceOffset = org.apache.arrow.computeir.flatbuf.Source.endSource(builder);
        int childRel = org.apache.arrow.computeir.flatbuf.Relation.createRelation(builder, org.apache.arrow.computeir.flatbuf.RelationImpl.Source, sourceOffset);

        int expressionsOffset = projections.serialize(builder);
        org.apache.arrow.computeir.flatbuf.Project.startProject(builder);
        org.apache.arrow.computeir.flatbuf.Project.addRel(builder, childRel);
        org.apache.arrow.computeir.flatbuf.Project.addExpressions(builder, expressionsOffset);
        int projectOffset = org.apache.arrow.computeir.flatbuf.Project.endProject(builder);

        int relationOffset = org.apache.arrow.computeir.flatbuf.Relation.createRelation(builder, org.apache.arrow.computeir.flatbuf.RelationImpl.Project, projectOffset);
        builder.finish(relationOffset);
        return builder.sizedByteArray(); // TODO: don't copy the data
    }

    public byte[] serializeCreateViewRequestBody(byte[] serializedSchema, byte[] serializedViewDetails)
    {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);
        int viewMetadataArrowBufferVectorOffset = CreateViewRequest.createViewMetadataArrowBufferVector(builder, serializedViewDetails);
        int schemaOffset = CreateViewRequest.createViewDataArrowSchemaVector(builder, serializedSchema);
        CreateViewRequest.startCreateViewRequest(builder);
        CreateViewRequest.addViewMetadataArrowBuffer(builder, viewMetadataArrowBufferVectorOffset);
        CreateViewRequest.addViewDataArrowSchema(builder, schemaOffset);
        int end = CreateViewRequest.endCreateViewRequest(builder);
        builder.finish(end);
        return builder.sizedByteArray();
    }

    public VectorSchemaRoot deserializeCreateViewRequestBody(byte[] bytes)
            throws IOException
    {
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        CreateViewRequest req = CreateViewRequest.getRootAsCreateViewRequest(wrap);
        int schemaLength = req.viewDataArrowSchemaLength();
        int mdLength = req.viewMetadataArrowBufferLength();

        byte[] newSchemArr = new byte[schemaLength];
        byte[] newMDArr = new byte[mdLength];
        req.viewDataArrowSchemaAsByteBuffer().get(newSchemArr);
        req.viewMetadataArrowBufferAsByteBuffer().get(newMDArr);

        InputStream input = new ByteArrayInputStream(newSchemArr);
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        MessageChannelReader messageChannelReader = new MessageChannelReader(new ReadChannel(Channels.newChannel(input)), allocator);

        MessageResult messageResult = messageChannelReader.readNext();
        if (messageResult == null) {
            messageChannelReader.readNext();
        }
        input = new ByteArrayInputStream(newMDArr);
        messageChannelReader = new MessageChannelReader(new ReadChannel(Channels.newChannel(input)), allocator);
        MessageResult result = messageChannelReader.readNext();
        Message message = result.getMessage();
        byte headerType = message.headerType();
        Schema schema2;
        if (headerType == MessageHeader.Schema) {
            schema2 = MessageSerializer.deserializeSchema(message);
        }
        else {
            throw new IOException("Unexpected header type. Expected Schema but got: " + headerType);
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

            VectorSchemaRoot root = VectorSchemaRoot.create(schema2, allocator);
            VectorLoader loader = new VectorLoader(root, NoCompressionCodec.Factory.INSTANCE);
            try (ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(message, bodyBuffer)) {
                loader.load(batch); // load `root` vectors from batch
            }
            return root;
        }
        else {
            throw new IOException("Unexpected header type. Expected RecordBatch but got: " + headerType);
        }
    }
}
