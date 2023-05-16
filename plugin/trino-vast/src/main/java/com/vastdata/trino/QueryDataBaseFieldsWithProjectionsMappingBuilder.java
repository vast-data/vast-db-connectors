/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.LinkedHashMap;
import java.util.List;

import static java.lang.String.format;

public class QueryDataBaseFieldsWithProjectionsMappingBuilder
{
    private final LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>> map = new LinkedHashMap<>();
    private int responseIndex;

    public QueryDataBaseFieldsWithProjectionsMappingBuilder put(Field field, List<Integer> projection)
    {
        Integer existingMapping = map.computeIfAbsent(field, f -> new LinkedHashMap<>(1)).putIfAbsent(projection, responseIndex++);
        if (existingMapping != null) {
            throw new IllegalStateException(format("Found duplicate projection path mapping for field: %s, path: %s, index: %s", field, projection, responseIndex));
        }
        return this;
    }

    public LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>> build()
    {
        return map;
    }
}
