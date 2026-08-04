/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import com.vastdata.client.error.VastUserException;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

class DuplicatePropertyValidator
        implements VastPropertyValidator
{
    private final String propertyName;
    private final List<String> propertyValues;

    @SuppressWarnings("checkstyle:RegexpSingleline")
    DuplicatePropertyValidator(String propertyName, List<String> propertyValues)
    {
        this.propertyName = Objects.requireNonNull(propertyName,
                "propertyName cannot be null");
        this.propertyValues = Objects.requireNonNull(propertyValues,
                "propertyValues cannot be null");
    }

    @Override
    public void validate()
            throws VastUserException
    {
        if (propertyValues.isEmpty()) {
            return;
        }

        Set<String> seenColumns = new HashSet<>();
        for (String col : propertyValues) {
            if (!seenColumns.add(col)) {
                throw new VastUserException(
                        String.format("Each column can only appear once in %s",
                                propertyName));
            }
        }
    }
}
