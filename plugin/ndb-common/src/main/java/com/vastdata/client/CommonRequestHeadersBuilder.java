/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class CommonRequestHeadersBuilder
        implements VastRequestHeadersBuilder
{
    private final Multimap<String, String> headers = HashMultimap.create(RequestsHeaders.values().length, 1);
    private final Supplier<String> clientTagSupplier;

    public CommonRequestHeadersBuilder(Supplier<String> clientTagSupplier)
    {
        this.clientTagSupplier = clientTagSupplier;
    }

    @Override
    public Multimap<String, String> build()
    {
        headers.put(RequestsHeaders.TABULAR_CLIENT_NAME.getHeaderName(), this.clientTagSupplier.get());
        headers.put(RequestsHeaders.TABULAR_API_VERSION_ID.getHeaderName(), VastRequestHeadersBuilder.VAST_CLIENT_API_VERSION);
        return headers;
    }

    @Override
    public VastRequestHeadersBuilder withTransaction(VastTransaction tx)
    {
        headers.put(RequestsHeaders.TABULAR_TRANSACTION_ID.getHeaderName(), Long.toUnsignedString(tx.getId()));
        return this;
    }

    @Override
    public VastRequestHeadersBuilder withCaseSensitive(boolean sensitive)
    {
        headers.put(RequestsHeaders.TABULAR_CASE_SENSITIVE.getHeaderName(), Boolean.toString(sensitive));
        return this;
    }

    @Override
    public VastRequestHeadersBuilder withNextKey(Long nextKey)
    {
        Preconditions.checkArgument(!Objects.isNull(nextKey), "Next iteration key was not provided");
        headers.put(RequestsHeaders.TABULAR_NEXT_KEY.getHeaderName(), String.valueOf(nextKey));
        return this;
    }

    @Override
    public VastRequestHeadersBuilder withMaxKeys(long maxKeys)
    {
        headers.put(RequestsHeaders.TABULAR_MAX_KEYS.getHeaderName(), Long.toString(maxKeys));
        return this;
    }

    @Override
    public VastRequestHeadersBuilder withContentLength(int length)
    {
        headers.put(RequestsHeaders.TABULAR_CONTENT_LENGTH.getHeaderName(), Long.toString(length));
        return this;
    }

    @Override
    public VastRequestHeadersBuilder withReadOnlyTransaction(boolean readOnly)
    {
        headers.put(RequestsHeaders.TABULAR_TRANSACTION_READ_ONLY.getHeaderName(), Boolean.toString(readOnly));
        return this;
    }

    @Override
    public VastRequestHeadersBuilder withExactMatch(String nameToMatch)
    {
        headers.put(RequestsHeaders.TABULAR_EXACT_MATCH.getHeaderName(), nameToMatch);
        return this;
    }

    @Override
    public VastRequestHeadersBuilder withTraceToken(VastTraceToken traceToken)
    {
        headers.put(RequestsHeaders.TABULAR_TRACE_TOKEN.getHeaderName(), traceToken.toString());
        return this;
    }

    @Override
    public VastRequestHeadersBuilder withSorting(List<Integer> sortedColumns)
    {
        for (int i = 0; i < sortedColumns.size(); i++) {
            headers.put(String.format(RequestsHeaders.TABULAR_SORTED_TEMPLATE.getHeaderName(), i), sortedColumns.get(i).toString());
        }
        return this;
    }
}
