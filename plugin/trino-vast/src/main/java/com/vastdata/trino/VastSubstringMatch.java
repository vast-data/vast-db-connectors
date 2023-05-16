/* Copyright (C) Vast Data Ltd. */

package com.vastdata.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.lang.String.format;

public class VastSubstringMatch
{
    private final VastColumnHandle column;
    private final String pattern;

    @JsonCreator
    public VastSubstringMatch(
            @JsonProperty("column") VastColumnHandle column,
            @JsonProperty("pattern") String pattern)
    {
        this.column = column;
        this.pattern = pattern;
    }

    @JsonProperty
    public VastColumnHandle getColumn()
    {
        return column;
    }

    @JsonProperty
    public String getPattern()
    {
        return pattern;
    }

    @Override
    public String toString()
    {
        return format("match_substring(%s, '%s')", column, pattern);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VastSubstringMatch that = (VastSubstringMatch) o;
        return Objects.equals(column, that.column) && Objects.equals(pattern, that.pattern);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, pattern);
    }
}
