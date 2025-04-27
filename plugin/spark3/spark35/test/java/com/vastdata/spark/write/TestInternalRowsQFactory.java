/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.InternalRow$;
import org.apache.spark.unsafe.types.UTF8String;
import org.testng.annotations.Test;
import scala.collection.immutable.List;
import scala.collection.mutable.Builder;

import java.util.Queue;

import static org.testng.Assert.assertEquals;

public class TestInternalRowsQFactory
{
    @Test
    public void testForUpdate()
    {
        Queue<InternalRow> unit = InternalRowsQFactory.forUpdate(10);
        Builder<Object, List<Object>> valuesBuilder = List.newBuilder();
        valuesBuilder.$plus$eq(1L);
        valuesBuilder.$plus$eq(UTF8String.fromString("aaa"));
        List<Object> values = valuesBuilder.result();
        InternalRow r1 = InternalRow$.MODULE$.apply(values);
        unit.add(r1);
        assertEquals(unit.peek(), r1);
        valuesBuilder = List.newBuilder();
        valuesBuilder.$plus$eq(2L);
        valuesBuilder.$plus$eq(UTF8String.fromString("bbb"));
        values = valuesBuilder.result();
        InternalRow r2 = InternalRow$.MODULE$.apply(values);
        unit.add(r2);
        assertEquals(unit.peek(), r1);
        valuesBuilder = List.newBuilder();
        valuesBuilder.$plus$eq(0L);
        valuesBuilder.$plus$eq(UTF8String.fromString("ccc"));
        values = valuesBuilder.result();
        InternalRow r0 = InternalRow$.MODULE$.apply(values);
        unit.add(r0);
        assertEquals(unit.peek(), r0);
    }
}
