/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.vdb.sdk;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Iterator;

import static java.util.Objects.requireNonNull;

/**
 * An iterator over query results, returning VectorSchemaRoot objects.
 * Each call to next() fetches the next batch of results.
 * The iterator ends when there are no more results to fetch.
 */
public class ResultIterator
        implements Iterator<VectorSchemaRoot> {
    private final Table table;

    ResultIterator(Table table)
    {
        this.table = requireNonNull(table);
    }

    @Override
    public boolean hasNext()
    {
        return !table.isFinished();
    }

    @Override
    public VectorSchemaRoot next()
    {
        return table.get();
    }
}
