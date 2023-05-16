/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.vastdata.mockserver.MockListBucketsReply;
import com.vastdata.mockserver.MockMapSchema;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.testng.Assert.assertEquals;

public class TestListBucketsResponseHandler
{
    @Test
    public void testParseBytes()
            throws Exception
    {
        ListBucketsResponseHandler unit = new ListBucketsResponseHandler();

        String empty = new MockListBucketsReply().apply(ImmutableMap.of());
        ByteArrayInputStream is = new ByteArrayInputStream(empty.getBytes(StandardCharsets.UTF_8));
        Set<String> parsedReplyBuckets = unit.parseBytes(is);
        assertEquals(parsedReplyBuckets.size(), 0);

        String bucket1 = "bucket1";
        String bucket2 = "bucket2";
        String buckets = new MockListBucketsReply().apply(
                ImmutableMap.of(bucket1, ImmutableSet.of(MockMapSchema.empty("schema1")),
                        bucket2, ImmutableSet.of(MockMapSchema.empty("schema2"))));
        is = new ByteArrayInputStream(buckets.getBytes(StandardCharsets.UTF_8));
        parsedReplyBuckets = unit.parseBytes(is);
        assertEquals(parsedReplyBuckets, ImmutableSet.of(bucket1, bucket2));
    }
}
