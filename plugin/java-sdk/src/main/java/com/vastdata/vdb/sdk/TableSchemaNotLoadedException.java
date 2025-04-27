package com.vastdata.vdb.sdk;

public class TableSchemaNotLoadedException
        extends RuntimeException
{
    public TableSchemaNotLoadedException()
    {
        super("Table schema not loaded");
    }
}
