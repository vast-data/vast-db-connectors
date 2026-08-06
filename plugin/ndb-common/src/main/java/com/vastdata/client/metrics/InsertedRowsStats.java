/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.metrics;

import java.util.concurrent.atomic.LongAdder;

public class InsertedRowsStats
{
    private final LongAdder rowsReceivedByBufferedInserter;
    private final LongAdder rowsPassedToByColumnInserter;
    private final LongAdder rowsToSend;
    private final LongAdder rowIdsReturnedFromInsert;

    public InsertedRowsStats()
    {
        this.rowsReceivedByBufferedInserter = new LongAdder();
        this.rowsPassedToByColumnInserter = new LongAdder();
        this.rowsToSend = new LongAdder();
        this.rowIdsReturnedFromInsert = new LongAdder();
    }

    public void addRowsReceivedByBufferedInserter(long count)
    {
        rowsReceivedByBufferedInserter.add(count);
    }

    public void addRowsPassedToByColumnInserter(long count)
    {
        rowsPassedToByColumnInserter.add(count);
    }

    public void addRowsToSend(long count)
    {
        rowsToSend.add(count);
    }

    public void addRowIdsReturnedFromInsert(long count)
    {
        rowIdsReturnedFromInsert.add(count);
    }

    public long getRowsReceivedByBufferedInserter()
    {
        return rowsReceivedByBufferedInserter.sum();
    }

    public long getRowsPassedToByColumnInserter()
    {
        return rowsPassedToByColumnInserter.sum();
    }

    public long getRowsToSend()
    {
        return rowsToSend.sum();
    }

    public long getRowIdsReturnedFromInsert()
    {
        return rowIdsReturnedFromInsert.sum();
    }

    @Override
    public String toString()
    {
        return "InsertedRowsStats{" + "rowsReceivedByBufferedInserter=" + getRowsReceivedByBufferedInserter() + ", rowsPassedToByColumnInserter=" + getRowsPassedToByColumnInserter() + ", rowsToSend=" + getRowsToSend() + ", rowIdsReturnedFromInsert=" + getRowIdsReturnedFromInsert() + '}';
    }
}
