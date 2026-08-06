/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import io.airlift.log.Logger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.vastdata.client.error.VastExceptionFactory.serializationException;

public class VastArrowPayloadSerializer<T>
        implements BiFunction<T, BufferAllocator, Optional<byte[]>>
{
    private static final Logger LOG = Logger.get(VastPayloadSerializer.class);

    private static final BiFunction<Schema, BufferAllocator, byte[]> schemaFunction = (schema, allocator) -> {
        try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema,
                allocator)) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(
                    vectorSchemaRoot, null, outputStream)) {
                writer.start();
                writer.writeBatch();
            }
            catch (IOException e) {
                throw serializationException("Failed serializing schema", e);
            }
            return outputStream.toByteArray();
        }
    };

    private final BiFunction<T, BufferAllocator, byte[]> function;

    private VastArrowPayloadSerializer(
            BiFunction<T, BufferAllocator, byte[]> function)
    {
        this.function = function;
    }

    public static VastArrowPayloadSerializer<Schema> getInstanceForSchema()
    {
        return new VastArrowPayloadSerializer<>(schemaFunction);
    }

    @Override
    public Optional<byte[]> apply(T o, BufferAllocator allocator)
    {
        try (BufferAllocator serializeAllocator = allocator.newChildAllocator(
                "serialize", 0, Long.MAX_VALUE)) {
            if (Objects.isNull(o)) {
                return Optional.empty();
            }
            return Optional.of(function.apply(o, allocator));
        }
    }
}
