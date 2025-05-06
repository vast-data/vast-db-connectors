/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.vastdata.client.schema.VastMetadataUtils.SORTED_BY_PROPERTY;
import static com.vastdata.client.schema.VastMetadataUtils.colNamesToIndex;;

public class AlterTableContext
{
    private final Optional<String> name;
    private final Optional<Map<String, Optional<Object>>> properties;
    private final List<String> columnNames;

    public AlterTableContext(String name, Map<String, Optional<Object>> properties, List<String> columnNames)
    {
        this.name = Optional.ofNullable(name);
        this.properties = Optional.ofNullable(properties);
	this.columnNames = columnNames;
    }

    public AlterTableContext(String name, Map<String, Optional<Object>> properties)
    {
	this(name, properties, null);
    }

    public Optional<Map<String, Optional<Object>>> getProperties()
    {
        return properties;
    }

    public Optional<String> getName()
    {
        return name;
    }

    public Optional<List<Integer>> getSortKey()
    {
	if (!properties.isPresent()) {
	    return Optional.empty();
	}
	Optional<Object> rawOptionalSortedBy = properties.get().get(SORTED_BY_PROPERTY);
	if (rawOptionalSortedBy == null || !rawOptionalSortedBy.isPresent()) {
	    return Optional.empty();
	}
	List<String> rawSortedBy = (List<String>)rawOptionalSortedBy.get();
	if (rawSortedBy == null) {
	    return Optional.empty();
	}
	return Optional.of(colNamesToIndex(columnNames, rawSortedBy));
    }
}
