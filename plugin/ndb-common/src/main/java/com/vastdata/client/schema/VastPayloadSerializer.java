/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.vastdata.client.error.VastExceptionFactory.serializationException;

public class VastPayloadSerializer<T>
        implements Function<T, Optional<byte[]>>
{
    private static final Logger LOG = Logger.get(VastPayloadSerializer.class);

    @SuppressWarnings("rawtypes")
    private static final Function<Map, byte[]> mapFunction = map -> {
        try {
            return new ObjectMapper().writeValueAsBytes(map);
        }
        catch (JsonProcessingException e) {
            throw serializationException("Failed serializing parameters o", e);
        }
    };

    private static final RootAllocator allocator = new RootAllocator();
    private static final Function<Schema, byte[]> schemaFunction = schema -> {
        try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, outputStream)) {
                writer.start();
                writer.writeBatch();
            }
            catch (IOException e) {
                throw serializationException("Failed serializing schema", e);
            }
            return outputStream.toByteArray();
        }
    };

    private static final Function<VectorSchemaRoot, byte[]> recordBatchFunction = vectorSchemaRoot -> {
        int rows = vectorSchemaRoot.getRowCount();
        Schema schema = vectorSchemaRoot.getSchema();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, outputStream)) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        catch (IOException e) {
            throw serializationException("Failed serializing RecordBatch", e);
        }
        LOG.debug("Serialized RecordBatch (%d rows, %s) to %d bytes", rows, schema, outputStream.size());
        return outputStream.toByteArray();
    };

    private final Function<T, byte[]> function;

    private VastPayloadSerializer(Function<T, byte[]> function)
    {
        this.function = function;
    }

    @SuppressWarnings("rawtypes")
    public static VastPayloadSerializer<Map> getInstanceForMap()
    {
        return new VastPayloadSerializer<>(mapFunction);
    }

    public static VastPayloadSerializer<Schema> getInstanceForSchema()
    {
        return new VastPayloadSerializer<>(schemaFunction);
    }

    public static VastPayloadSerializer<VectorSchemaRoot> getInstanceForRecordBatch()
    {
        return new VastPayloadSerializer<>(recordBatchFunction);
    }

    @Override
    public Optional<byte[]> apply(T o)
    {
        if (Objects.isNull(o)) {
            return Optional.empty();
        }
        return Optional.of(function.apply(o));
    }
}
