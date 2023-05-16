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
        VastTableHandle tableHandle = new VastTableHandle("schemaName", "tableName", false).forDelete();

        JsonCodec<VastTableHandle> codec = jsonCodec(VastTableHandle.class);
        String json = codec.toJson(tableHandle);
        VastTableHandle copy = codec.fromJson(json);
        assertEquals(copy, tableHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(new VastTableHandle("schema", "table", false), new VastTableHandle("schema", "table", false))
                .addEquivalentGroup(new VastTableHandle("schemaX", "table", false), new VastTableHandle("schemaX", "table", false))
                .addEquivalentGroup(new VastTableHandle("schemaX", "table", true), new VastTableHandle("schemaX", "table", true))
                .addEquivalentGroup(new VastTableHandle("schema", "tableX", false), new VastTableHandle("schema", "tableX", false))
                .check();
    }
}
