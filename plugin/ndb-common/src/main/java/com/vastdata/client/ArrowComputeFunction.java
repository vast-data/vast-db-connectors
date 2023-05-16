/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

public enum ArrowComputeFunction
{
    // From https://arrow.apache.org/docs/python/api/compute.html
    AND("and"),
    OR("or"),
    EQUAL("equal"),
    NOT_EQUAL("not_equal"),
    GREATER("greater"),
    GREATER_EQUAL("greater_equal"),
    LESS("less"),
    LESS_EQUAL("less_equal"),
    IS_NULL("is_null"),
    IS_VALID("is_valid"),
    MATCH_SUBSTRING("match_substring");

    private final String name;

    ArrowComputeFunction(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }
}
