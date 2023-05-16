/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestVastMetadataUtils
{
    @Test
    public void testNullMap()
    {
        String propertiesString = new VastMetadataUtils().getPropertiesString(null);
        assertEquals(propertiesString, "{}");
    }

    @Test
    public void testEmptyMap()
    {
        String propertiesString = new VastMetadataUtils().getPropertiesString(ImmutableMap.of());
        assertEquals(propertiesString, "{}");
    }

    @Test
    public void testMapWithContents()
            throws IOException
    {
        Map<String, Object> testMap = new HashMap<>();
        testMap.put("simpleValue", Integer.MAX_VALUE);
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("key", "val");
        testMap.put("compoundValue", nestedMap);
        String propertiesString = new VastMetadataUtils().getPropertiesString(testMap);
        assertEquals(new ObjectMapper().readValue(propertiesString.getBytes(UTF_8), Map.class), testMap);
    }
}
