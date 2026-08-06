/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import com.vastdata.client.error.VastUserException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.vastdata.client.schema.VastMetadataUtils.SORTED_BY_PROPERTY;
import static com.vastdata.client.schema.VastMetadataUtils.colNamesToIndex;
import static com.vastdata.client.schema.VastMetadataUtils.validateTableProperties;

public class AlterTableContext
{
    private final Optional<String> name;
    private final Optional<Map<String, Optional<Object>>> properties;
    private final List<String> columnNames;

    AlterTableContext(String name, Map<String, Optional<Object>> properties,
            List<String> columnNames)
    {
        this.name = Optional.ofNullable(name);
        this.properties = Optional.ofNullable(properties);
        this.columnNames = columnNames;
    }

    public static AlterTableContext create(String name,
            Map<String, Optional<Object>> properties, List<String> columnNames,
            boolean isCaseSensitive)
            throws VastUserException
    {
        parseAndValidateSortedBy(properties, columnNames, isCaseSensitive);
        return new AlterTableContext(name, properties, columnNames);
    }

    private static void parseAndValidateSortedBy(
            Map<String, Optional<Object>> properties, List<String> columnNames,
            boolean isCaseSensitive)
            throws VastUserException
    {
        if (properties == null || columnNames == null || !properties.containsKey(
                SORTED_BY_PROPERTY)) {
            return;
        }
        Optional<Object> rawOptionalSortedBy = properties.get(
                SORTED_BY_PROPERTY);
        if (rawOptionalSortedBy != null && rawOptionalSortedBy.isPresent() && rawOptionalSortedBy.get() != null) {
            @SuppressWarnings(
                    "unchecked") List<String> rawSortedBy = (List<String>) rawOptionalSortedBy.get();
            validateTableProperties(rawSortedBy, columnNames,
                    SORTED_BY_PROPERTY, isCaseSensitive);
        }
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
        Optional<Object> rawOptionalSortedBy = properties
                .get()
                .get(SORTED_BY_PROPERTY);
        if (rawOptionalSortedBy == null || !rawOptionalSortedBy.isPresent()) {
            return Optional.empty();
        }
        List<String> rawSortedBy = (List<String>) rawOptionalSortedBy.get();
        if (rawSortedBy == null) {
            return Optional.empty();
        }
        return Optional.of(colNamesToIndex(columnNames, rawSortedBy));
    }
}
