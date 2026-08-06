/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class QueryDataExtraParams
{
    public enum QueryDataExtraParamType
    {
        URL_PARAM,
        HEADER
    }

    private final Map<QueryDataExtraParams.QueryDataExtraParamType, Map<String, String>> extraQueryParams;

    public QueryDataExtraParams()
    {
        this(new HashMap<>());
    }

    @JsonCreator
    public QueryDataExtraParams(@JsonProperty("extraQueryParams") Map<QueryDataExtraParams.QueryDataExtraParamType, Map<String, String>> extraQueryParams)
    {
        this.extraQueryParams = extraQueryParams;
    }

    public void addExtraQueryParams(QueryDataExtraParams.QueryDataExtraParamType type, String key, String value)
    {
        Map<String, String> mergedParams = new HashMap<>();
        mergedParams.put(key, value);
        mergedParams.putAll(extraQueryParams.getOrDefault(type, new HashMap<>()));
        extraQueryParams.put(type, mergedParams);
    }

    @JsonProperty("extraQueryParams")
    public Map<QueryDataExtraParamType, Map<String, String>> getExtraQueryParams()
    {
        return extraQueryParams;
    }
}
