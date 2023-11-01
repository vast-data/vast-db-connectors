/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.VastSplitContext;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestVastSplit
{
    private final VastSplitContext context = new VastSplitContext(0, 1, 1, 1);
    private final VastSchedulingInfo info = new VastSchedulingInfo("123");
    private final List<URI> endpoints = List.of(URI.create("http://127.0.0.1:8080"));
    private final VastSplit split = new VastSplit(endpoints, context, info);

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<VastSplit> codec = jsonCodec(VastSplit.class);
        String json = codec.toJson(split);
        VastSplit copy = codec.fromJson(json);
        assertEquals(copy.getAddresses(), split.getAddresses());
        assertEquals(copy.getContext(), split.getContext());
        assertTrue(copy.isRemotelyAccessible());
    }

    @Test
    public void testInstanceSize()
            throws URISyntaxException
    {
        VastSchedulingInfo schedulingInfo = new VastSchedulingInfo("id");
        VastSplitContext ctx = new VastSplitContext(0, 10, 5, 1);
        VastSplit split1 = new VastSplit(endpoints, ctx, schedulingInfo);
        List<URI> endpoints2 = new ArrayList<>(endpoints);
        endpoints2.add(new URI("http://localhost:8080"));
        VastSplit split2 = new VastSplit(endpoints2, ctx, schedulingInfo);
        assertTrue(split1.getRetainedSizeInBytes() < split2.getRetainedSizeInBytes(), "expected second split size to be larger");
    }
}
