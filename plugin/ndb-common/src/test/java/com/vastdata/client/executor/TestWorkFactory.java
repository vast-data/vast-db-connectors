/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.executor;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestWorkFactory
{
    @Test
    public void testFromCollection()
    {
        List<String> testCollection = ImmutableList.of("S1", "S2", "S3", "S4", "S5");
        ArrayList<Object> resultTestCollection = new ArrayList<>(testCollection.size());
        Supplier<Function<URI, Object>> functionSupplier = WorkFactory.fromCollection(testCollection, (s, uri) -> s);
        for (int i = 0; i < testCollection.size(); i++) {
            resultTestCollection.add(functionSupplier.get().apply(null));
        }
        assertEquals(resultTestCollection, testCollection);
        assertNull(functionSupplier.get());
    }
}
