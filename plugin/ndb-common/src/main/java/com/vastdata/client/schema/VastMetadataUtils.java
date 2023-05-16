/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class VastMetadataUtils
{
    private static final byte[] EMPTY_MAP = "{}".getBytes(StandardCharsets.UTF_8);

    public String getPropertiesString(Map<String, Object> properties)
    {
        VastPayloadSerializer<Map> instanceForMap = VastPayloadSerializer.getInstanceForMap();
        return new String(instanceForMap.apply(properties).orElse(EMPTY_MAP), StandardCharsets.UTF_8);
    }
}
