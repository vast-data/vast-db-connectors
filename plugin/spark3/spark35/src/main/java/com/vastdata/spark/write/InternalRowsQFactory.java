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
    private static final Function<Integer, Queue<InternalRow>> ROW_ID_BASED_PRIORITY_QUEUE_FACTORY =
            size -> new PriorityBlockingQueue<>(size, Comparator.comparingLong((InternalRow o) -> o.getLong(0)));

    private InternalRowsQFactory() {}

    public static Queue<InternalRow> forDelete(int size) {
        return ROW_ID_BASED_PRIORITY_QUEUE_FACTORY.apply(size);
    }

    public static Queue<InternalRow> forUpdate(int size) {
        return ROW_ID_BASED_PRIORITY_QUEUE_FACTORY.apply(size);
    }

    public static Queue<InternalRow> forInsert(int size) {
        return new LinkedBlockingQueue<>(size);
    }
}
