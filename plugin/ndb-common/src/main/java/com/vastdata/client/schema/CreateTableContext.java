/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.vastdata.client.schema.VastMetadataUtils.SORTED_BY_PROPERTY;
import static com.vastdata.client.schema.VastMetadataUtils.colNamesToIndex;;

public class CreateTableContext
{
    private final String schemaName;
    private final String tableName;
    private final List<Field> fields;
    private final Optional<String> comment;
    private final Map<String, Object> properties;

    public CreateTableContext(String schemaName, String tableName, List<Field> fields,
            Optional<String> comment, Map<String, Object> properties)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.fields = fields;
        this.comment = comment;
        this.properties = properties;
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
	List<String> rawSortedBy = (List<String>)properties.get(SORTED_BY_PROPERTY);
	if (rawSortedBy == null) {
	    return new ArrayList<>();
	}
	return colNamesToIndex(fields.stream().map(Field::getName).collect(Collectors.toList()), rawSortedBy);
    }
}
