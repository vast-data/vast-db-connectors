/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver;

import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public final class MockMapSchema
{
    private final String name;
    private final Map<String, MockTable> tables;
    private final Map<String, MockView> views;
    private final Map<String, String> properties;

    private MockMapSchema(String name, Map<String, MockTable> tables, Map<String, MockView> views, Map<String, String> properties)
    {
        this.name = name;
        this.tables = tables;
        this.views = views;
        this.properties = properties;
    }

    public static MockMapSchema empty(String name)
    {
        checkArgument(!Strings.isNullOrEmpty(name));
        return new MockMapSchema(name, new HashMap<>(), new HashMap<>(), new HashMap<>());
    }

    public String getName()
    {
        return name;
    }

    public Map<String, MockTable> getTables()
    {
        return tables;
    }

    public Map<String, MockView> getViews()
    {
        return views;
    }

    public Map<String, String> getProperties()
    {
        return properties;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(MockMapSchema.class.getSimpleName())
                .add("name", name)
                .add("tables", tables)
                .add("views", views)
                .add("properties", properties)
                .toString();
    }
}
