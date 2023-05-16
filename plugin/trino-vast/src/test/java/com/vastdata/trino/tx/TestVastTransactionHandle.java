/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.tx;

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.HashSet;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestVastTransactionHandle
{
    public static final long TRANS_ID = Long.parseUnsignedLong("514026084031791104");
    private final VastTransactionHandle trans = new VastTransactionHandle(TRANS_ID, false, true);

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<VastTransactionHandle> codec = jsonCodec(VastTransactionHandle.class);
        String json = codec.toJson(trans);
        VastTransactionHandle copy = codec.fromJson(json);
        assertEquals(copy, trans);
    }

    @Test
    public void testEquals()
    {
        HashSet<VastTransactionHandle> transSet = new HashSet<>();
        transSet.add(trans);
        VastTransactionHandle testTrans1 = new VastTransactionHandle(Long.parseUnsignedLong("514026084031791105"), false, true);
        assertFalse(transSet.contains(testTrans1));
        VastTransactionHandle testTrans2 = new VastTransactionHandle(TRANS_ID, true, false);
        assertTrue(transSet.contains(testTrans2));
    }
}
