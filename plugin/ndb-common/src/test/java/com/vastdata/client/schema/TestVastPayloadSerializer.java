/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestVastPayloadSerializer
{
    @Test
    public void testApplyNull()
    {
        Optional<byte[]> apply = VastPayloadSerializer.getInstanceForMap().apply(null);
        assertTrue(!apply.isPresent());
    }

    @Test
    public void testApplyEmptyMap()
    {
        Optional<byte[]> apply = VastPayloadSerializer.getInstanceForMap().apply(ImmutableMap.of());
        assertTrue(apply.isPresent());
        assertEquals(new String(apply.get(), StandardCharsets.UTF_8), "{}");
    }

    @Test
    public void testApplySimpleMap()
    {
        Optional<byte[]> apply = VastPayloadSerializer.getInstanceForMap().apply(ImmutableMap.of("akey", "avalue"));
        assertTrue(apply.isPresent());
        assertEquals(new String(apply.get(), StandardCharsets.UTF_8), "{\"akey\":\"avalue\"}");
    }
}
