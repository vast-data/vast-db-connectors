/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.VastSplitContext;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import org.openjdk.jol.info.ClassLayout;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

public class VastSplit
        implements ConnectorSplit
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(VastSplit.class).instanceSize();

    private final VastSplitContext context;
    private final List<URI> endpoints;
    private final VastSchedulingInfo schedulingInfo;

    @JsonCreator
    public VastSplit(
            @JsonProperty("endpoints") List<URI> endpoints,
            @JsonProperty("context") VastSplitContext context,
            @JsonProperty("schedulingInfo") VastSchedulingInfo schedulingInfo)
    {
        this.endpoints = requireNonNull(endpoints, "addresses is null");
        this.context = requireNonNull(context, "context is null");
        this.schedulingInfo = requireNonNull(schedulingInfo, "schedulingInfo is null");
    }

    @JsonProperty
    public List<URI> getEndpoints()
    {
        return endpoints;
    }

    @JsonProperty
    public VastSplitContext getContext()
    {
        return context;
    }

    @JsonProperty
    public VastSchedulingInfo getSchedulingInfo()
    {
        return schedulingInfo;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return endpoints.stream().map(HostAddress::fromUri).collect(Collectors.toList());
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(endpoints, uri -> estimatedSizeOf(uri.toString()));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("endpoints", endpoints)
                .add("context", context)
                .toString();
    }
}
