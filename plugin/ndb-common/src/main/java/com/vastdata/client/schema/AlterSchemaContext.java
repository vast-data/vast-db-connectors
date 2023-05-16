/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import java.util.Map;
import java.util.Optional;

public class AlterSchemaContext
{
    private final Optional<String> newName;
    private final Optional<Map<String, Optional<Object>>> properties;

    public AlterSchemaContext(String newName, Map<String, Optional<Object>> properties)
    {
        this.newName = Optional.of(newName);
        this.properties = Optional.ofNullable(properties);
    }

    public Optional<String> getNewName()
    {
        return newName;
    }

    public Optional<Map<String, Optional<Object>>> getProperties()
    {
        return properties;
    }
}
