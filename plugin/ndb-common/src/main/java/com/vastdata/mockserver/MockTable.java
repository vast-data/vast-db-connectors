/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver;

import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

import static com.google.common.base.Preconditions.checkArgument;

public final class MockTable
{
    private final String name;
    private final Map<String, Field> columns;

    private MockTable(String name, Map<String, Field> columns)
    {
        this.name = name;
        this.columns = columns;
    }

    public static MockTable empty(String name)
    {
        checkArgument(!Strings.isNullOrEmpty(name));
        return new MockTable(name, new LinkedHashMap<>());
    }

    public static MockTable withColumns(String name, Map<String, Field> columnsMap)
    {
        checkArgument(!Strings.isNullOrEmpty(name));
        checkArgument(!columnsMap.isEmpty());
        return new MockTable(name, columnsMap);
    }

    public String getName()
    {
        return name;
    }

    public Map<String, Field> getColumns()
    {
        return columns;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(MockTable.class.getSimpleName())
                .add("name", name)
                .add("columns", columns)
                .toString();
    }
}
