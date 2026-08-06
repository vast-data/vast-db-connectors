/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import com.vastdata.client.error.VastUserException;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

class ColumnExistsValidator
        implements VastPropertyValidator
{
    private final String propertyName;
    private final List<String> propertyValues;
    private final List<String> availableColumns;

    @SuppressWarnings("checkstyle:RegexpSingleline")
    ColumnExistsValidator(String propertyName, List<String> propertyValues,
            List<String> availableColumns)
    {
        this.propertyName = Objects.requireNonNull(propertyName,
                "propertyName cannot be null");
        this.propertyValues = Objects.requireNonNull(propertyValues,
                "propertyValues cannot be null");
        this.availableColumns = Objects.requireNonNull(availableColumns,
                "availableColumns cannot be null");
    }

    @Override
    public void validate()
            throws VastUserException
    {
        if (propertyValues.isEmpty()) {
            return;
        }
        Set<String> availableSet = new HashSet<>(availableColumns);
        for (String col : propertyValues) {
            if (!availableSet.contains(col)) {
                throw new VastUserException(String.format(
                        "Column '%s' specified in %s does not exist in the table",
                        col, propertyName));
            }
        }
    }
}
