/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vastdata.client.error.VastIOException;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleVastTransaction
        implements VastTransaction, Serializable
{
    private final long id;
    private final boolean readOnly;
    private final boolean autocommit;

    private final AtomicInteger operationCount = new AtomicInteger(0);

    public SimpleVastTransaction(long id, boolean readOnly, boolean autocommit)
    {
        this.id = id;
        this.readOnly = readOnly;
        this.autocommit = autocommit;
    }

    public SimpleVastTransaction()
    {
        this.id = 0;
        this.readOnly = true;
        this.autocommit = false;
    }

    @Override
    public long getId()
    {
        return id;
    }

    @Override
    public boolean isReadOnly()
    {
        return readOnly;
    }

    public boolean isAutocommit()
    {
        return autocommit;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleVastTransaction that = (SimpleVastTransaction) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }

    @Override
    public String toString()
    {
        try {
            return new ObjectMapper().writeValueAsString(this);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static SimpleVastTransaction fromString(String serialized)
            throws VastIOException
    {
        try {
            return new ObjectMapper().readValue(serialized, SimpleVastTransaction.class);
        }
        catch (JsonProcessingException e) {
            throw new VastIOException(e);
        }
    }

    @Override
    public VastTraceToken generateTraceToken(Optional<String> userTraceToken)
    {
        return new VastTraceToken(userTraceToken, this.id, operationCount.getAndIncrement());
    }
}
