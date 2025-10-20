/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.tx;

import com.vastdata.client.tx.VastTraceToken;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestVastTraceToken
{
    @Test
    public void testTestToString()
    {
        VastTraceToken unit = new VastTraceToken(Optional.empty(), 5L, 6);
        assertEquals(unit.toString(), "5:6");
        unit = new VastTraceToken(Optional.of("a user token"), 5L, 6);
        assertEquals(unit.toString(), "a user token:5:6");
    }
}
