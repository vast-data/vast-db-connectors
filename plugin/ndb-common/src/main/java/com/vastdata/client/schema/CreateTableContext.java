/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import com.vastdata.client.error.VastUserException;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.vastdata.client.schema.VastMetadataUtils.SORTED_BY_PROPERTY;
import static com.vastdata.client.schema.VastMetadataUtils.colNamesToIndex;
import static com.vastdata.client.schema.VastMetadataUtils.validateTableProperties;

public class CreateTableContext
{
    private final String schemaName;
    private final String tableName;
    private final List<Field> fields;
    private final Optional<String> comment;
    private final Map<String, Object> properties;
    private final Map<String, String> partitionDefs;

    CreateTableContext(String schemaName, String tableName, List<Field> fields,
            Optional<String> comment, Map<String, Object> properties,
            Map<String, String> partitionDefs)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.fields = fields;
        this.comment = comment;
        this.properties = properties;
        this.partitionDefs = partitionDefs;
    }

    public static CreateTableContext create(String schemaName, String tableName,
            List<Field> fields, Optional<String> comment,
            Map<String, Object> properties, Map<String, String> partitionDefs,
            boolean isCaseSensitive)
            throws VastUserException
    {
        parseAndValidateSortedBy(properties, fields, isCaseSensitive);
        return new CreateTableContext(schemaName, tableName, fields, comment,
                properties, partitionDefs);
    }

    private static void parseAndValidateSortedBy(Map<String, Object> properties,
            List<Field> fields, boolean isCaseSensitive)
            throws VastUserException
    {
        if (properties == null || fields == null || properties.get(
                SORTED_BY_PROPERTY) == null) {
            return;
        }
        @SuppressWarnings(
                "unchecked") List<String> rawSortedBy = (List<String>) properties.get(
                SORTED_BY_PROPERTY);
        List<String> availableColumns = fields
                .stream()
                .map(Field::getName)
                .collect(Collectors.toList());
        validateTableProperties(rawSortedBy, availableColumns,
                SORTED_BY_PROPERTY, isCaseSensitive);
    }

    public String getSchemaName()
    {
        return this.schemaName;
    }

    public String getTableName()
    {
        return this.tableName;
    }

    List<Field> getFields()
    {
        return fields;
    }

    public List<Integer> getSortKey()
    {
        List<String> rawSortedBy = (List<String>) properties.get(
                SORTED_BY_PROPERTY);
        if (rawSortedBy == null) {
            return new ArrayList<>();
        }
        return colNamesToIndex(fields
                .stream()
                .map(Field::getName)
                .collect(Collectors.toList()), rawSortedBy);
    }

    public Map<String, String> getPartitionDefs()
    {
        if (partitionDefs == null || partitionDefs.isEmpty()) {
            return null;
        }
        return partitionDefs;
    }
}
