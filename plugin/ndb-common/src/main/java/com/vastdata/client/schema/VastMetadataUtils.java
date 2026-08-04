/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import com.vastdata.client.error.VastUserException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class VastMetadataUtils
{
    public static final String SORTED_BY_PROPERTY = "sorted_by";
    public static final String PARTITIONED_BY_PROPERTY = "partitioning";
    private static final byte[] EMPTY_MAP = "{}".getBytes(
            StandardCharsets.UTF_8);

    // TODO: as Trino is case insensitive, this is doing case insensitive compare
    // right now
    // But Spark can be case sensitive. So this should be rethought when Spark
    // support is added
    // to elysium
    public static List<Integer> colNamesToIndex(List<String> allColumnNames,
            List<String> columnNames)
    {
        List<Integer> rv = new ArrayList<>(columnNames.size());
        for (String n : columnNames) {
            for (int i = 0; i < allColumnNames.size(); i++) {
                if (allColumnNames.get(i).equalsIgnoreCase(n)) {
                    rv.add(i);
                    break;
                }
            }
        }
        return rv;
    }

    /**
     * Creates a new list with each string lowercased. Null elements are
     * preserved as null.
     */
    private static List<String> toLowerCaseCopy(List<String> list)
    {
        if (list == null) {
            return null;
        }
        return list
                .stream()
                .map(s -> s == null ?
                        null :
                        s.toLowerCase(java.util.Locale.ENGLISH))
                .collect(Collectors.toList());
    }

    public static void validateTableProperties(List<String> propertyValues,
            List<String> availableColumns, String propertyName,
            boolean isCaseSensitive)
            throws VastUserException
    {
        List<String> valuesForValidation = propertyValues;
        List<String> columnsForValidation = availableColumns;
        if (!isCaseSensitive) {
            valuesForValidation = toLowerCaseCopy(propertyValues);
            columnsForValidation = toLowerCaseCopy(availableColumns);
        }
        List<VastPropertyValidator> validators = Arrays.asList(
                new DuplicatePropertyValidator(propertyName,
                        valuesForValidation),
                new ColumnExistsValidator(propertyName, valuesForValidation,
                        columnsForValidation));

        for (VastPropertyValidator validator : validators) {
            validator.validate();
        }
    }

    public String getPropertiesString(Map<String, Object> properties)
    {
        VastPayloadSerializer<Map> instanceForMap = VastPayloadSerializer.getInstanceForMap();
        return new String(instanceForMap.apply(properties).orElse(EMPTY_MAP),
                StandardCharsets.UTF_8);
    }
}
