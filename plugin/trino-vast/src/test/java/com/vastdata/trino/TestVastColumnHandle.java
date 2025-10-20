/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import io.airlift.json.JsonCodec;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.testng.annotations.Test;

import java.util.List;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestVastColumnHandle
{
    private final Field field = Field.nullable("a", new ArrowType.Utf8());
    private final VastColumnHandle handle = VastColumnHandle.fromField(field);

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<VastColumnHandle> codec = jsonCodec(VastColumnHandle.class);
        String json = codec.toJson(handle);
        VastColumnHandle copy = codec.fromJson(json);
        assertEquals(copy, handle);
        assertEquals(copy.getField(), field);
        assertEquals(copy.getProjectionPath(), List.of());
    }
}
