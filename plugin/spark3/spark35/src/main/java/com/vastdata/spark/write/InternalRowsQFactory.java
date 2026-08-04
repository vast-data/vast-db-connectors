/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write;

import org.apache.spark.sql.catalyst.InternalRow;

import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Function;

public final class InternalRowsQFactory
{
    private static final Function<Integer, Queue<InternalRow>> INT64_ROW_ID_BASED_PRIORITY_QUEUE_FACTORY = size -> new PriorityBlockingQueue<>(
            size, Comparator.comparingLong((InternalRow o) -> o.getLong(0)));

    private static final Function<Integer, Queue<InternalRow>> DEC128_ROW_ID_BASED_PRIORITY_QUEUE_FACTORY = size -> new PriorityBlockingQueue<>(
            size,
            Comparator.comparing((InternalRow o) -> o.getDecimal(0, 38, 0)));

    private InternalRowsQFactory()
    {
    }

    public static Queue<InternalRow> forDelete(int size, boolean complexRowID)
    {
        return complexRowID ? DEC128_ROW_ID_BASED_PRIORITY_QUEUE_FACTORY.apply(
                size) : INT64_ROW_ID_BASED_PRIORITY_QUEUE_FACTORY.apply(size);
    }

    public static Queue<InternalRow> forUpdate(int size, boolean complexRowID)
    {
        return complexRowID ? DEC128_ROW_ID_BASED_PRIORITY_QUEUE_FACTORY.apply(
                size) : INT64_ROW_ID_BASED_PRIORITY_QUEUE_FACTORY.apply(size);
    }

    public static Queue<InternalRow> forInsert(int size)
    {
        return new LinkedBlockingQueue<>(size);
    }
}
