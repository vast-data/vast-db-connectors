package com.vastdata.mockserver;

import org.apache.arrow.vector.VectorSchemaRoot;

public class MockView
{
    private final String name;
    private final VectorSchemaRoot details;

    public MockView(String name, VectorSchemaRoot details) {
        this.name = name;
        this.details = details;
    }

    public String getName()
    {
        return name;
    }

    public VectorSchemaRoot getDetails()
    {
        return details;
    }
}
