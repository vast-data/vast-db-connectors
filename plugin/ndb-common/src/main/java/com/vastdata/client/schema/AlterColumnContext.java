/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import java.util.Map;
import java.util.Optional;

public class AlterColumnContext
{
    private final String name;
    private final Optional<String> newName;
    private final Optional<Map<String, String>> properties;
    private final Optional<String> serializedStats;

    public AlterColumnContext(String name, String newName,
            Map<String, String> properties, String serializedStats)
    {
        this.name = name;
        this.newName = Optional.ofNullable(newName);
        this.properties = Optional.ofNullable(properties);
        this.serializedStats = Optional.ofNullable(serializedStats);
    }

    public Optional<String> getNewName()
    {
        return newName;
    }

    public String getName()
    {
        return name;
    }

    public Optional<Map<String, String>> getProperties()
    {
        return properties;
    }

    public Optional<String> getSerializedStats()
    {
        return serializedStats;
    }
}
