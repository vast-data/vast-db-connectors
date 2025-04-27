/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

public class DropViewContext
{
    private final String schemaName;

    private final String viewName;

    public DropViewContext(final String schemaName, final String viewName)
    {
        this.schemaName = schemaName;
        this.viewName = viewName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getViewName()
    {
        return viewName;
    }
}
