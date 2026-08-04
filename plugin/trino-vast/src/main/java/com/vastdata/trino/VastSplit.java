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
import io.trino.spi.predicate.TupleDomain;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.vastdata.trino.ClassInstanceSize.sizeOf;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

public class VastSplit
        implements ConnectorSplit
{
    public static final AtomicInteger SPLIT_COUNTER = new AtomicInteger(0);
    private static final long INSTANCE_SIZE = sizeOf(VastSplit.class);

    private final VastSplitContext context;
    private final List<URI> endpoints;
    private final VastSchedulingInfo schedulingInfo;
    private final TupleDomain<VastColumnHandle> filters;
    private final HostAddress address;
    private final String traceToken;
    private final long retainedSize;

    @JsonCreator
    public VastSplit(@JsonProperty("address") HostAddress address,
                     @JsonProperty("endpoints") List<URI> endpoints,
                     @JsonProperty("context") VastSplitContext context,
                     @JsonProperty("schedulingInfo") VastSchedulingInfo schedulingInfo,
                     @JsonProperty("filters") TupleDomain<VastColumnHandle> filters,
                     @JsonProperty("traceToken") String traceToken)
    {
        this.address = address;
        this.endpoints = requireNonNull(endpoints, "endpoints is null");
        this.context = requireNonNull(context, "context is null");
        this.schedulingInfo = schedulingInfo;
        this.filters = requireNonNull(filters, "filters is null");
        this.traceToken = requireNonNull(traceToken, "traceToken is null");
        this.retainedSize = INSTANCE_SIZE + estimatedSizeOf(endpoints, uri -> estimatedSizeOf(uri.toString()));
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

    @JsonProperty
    public TupleDomain<VastColumnHandle> getFilters()
    {
        return filters;
    }

    @JsonProperty
    public String getTraceToken()
    {
        return traceToken;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return address != null ? List.of(address) : List.of();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSize;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("endpoints", endpoints)
                .add("address", address)
                .add("context", context)
                .add("filters", filters)
                .add("traceToken", traceToken)
                .toString();
    }
}
