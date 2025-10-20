/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import io.airlift.json.JsonCodec;
import io.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestVastTableHandle
{
    @Test
    public void testJsonRoundTrip()
    {
        VastTableHandle tableHandle = new VastTableHandle("schemaName", "tableName", "id", false).forDelete();

        JsonCodec<VastTableHandle> codec = jsonCodec(VastTableHandle.class);
        String json = codec.toJson(tableHandle);
        VastTableHandle copy = codec.fromJson(json);
        assertEquals(copy, tableHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(new VastTableHandle("schema", "table", "id", false), new VastTableHandle("schema", "table", "id", false))
                .addEquivalentGroup(new VastTableHandle("schemaX", "table", "id", false), new VastTableHandle("schemaX", "table", "id", false))
                .addEquivalentGroup(new VastTableHandle("schemaX", "table", "id", true), new VastTableHandle("schemaX", "table", "id", true))
                .addEquivalentGroup(new VastTableHandle("schema", "tableX", "id", false), new VastTableHandle("schema", "tableX", "id", false))
                .check();
    }
}
