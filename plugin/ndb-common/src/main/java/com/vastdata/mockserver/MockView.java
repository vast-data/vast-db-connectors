/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.mockserver;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class MockView
{
    private final String name;
    private final VectorSchemaRoot details;
    private final Schema viewDataSchema;

    public MockView(String name, VectorSchemaRoot details,
            Schema viewDataSchema)
    {
        this.name = name;
        this.details = details;
        this.viewDataSchema = viewDataSchema;
    }

    public String getName()
    {
        return name;
    }

    public VectorSchemaRoot getDetails()
    {
        return details;
    }

    public Schema getViewDataSchema()
    {
        return viewDataSchema;
    }
}
