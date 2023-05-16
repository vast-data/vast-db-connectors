/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import io.airlift.http.client.HeaderName;

import java.io.Serializable;
import java.util.Collection;

public class VastSchedulingInfo
        implements Serializable
{
    private static final HeaderName HEADER_NAME = HeaderName.of("tabular-schedule-id");
    private final String id; // opaque identifier to be passed back to VAST

    public static VastSchedulingInfo create(VastResponse response)
    {
        Collection<String> values = response.getHeaders().get(HEADER_NAME);
        return new VastSchedulingInfo(Iterators.getOnlyElement(values.iterator()));
    }

    @JsonCreator
    public VastSchedulingInfo(@JsonProperty("id") String id)
    {
        this.id = id;
    }

    @JsonProperty
    public String getId()
    {
        return id;
    }

    public void updateHeaders(Multimap<String, String> headers)
    {
        headers.put(HEADER_NAME.toString(), id);
    }
}
