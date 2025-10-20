/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import java.util.Map;
import java.util.Optional;

public class AlterTableContext
{
    private final Optional<String> name;
    private final Optional<Map<String, Optional<Object>>> properties;

    public AlterTableContext(String name, Map<String, Optional<Object>> properties)
    {
        this.name = Optional.ofNullable(name);
        this.properties = Optional.ofNullable(properties);
    }

    public Optional<Map<String, Optional<Object>>> getProperties()
    {
        return properties;
    }

    public Optional<String> getName()
    {
        return name;
    }
}
