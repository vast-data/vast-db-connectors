/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.google.common.collect.Multimap;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;

import java.util.List;

public interface VastRequestHeadersBuilder
{
    String VAST_CLIENT_API_VERSION = "1";

    Multimap<String, String> build();

    VastRequestHeadersBuilder withTransaction(VastTransaction tx);

    VastRequestHeadersBuilder withCaseSensitive(boolean sensitive);

    VastRequestHeadersBuilder withNextKey(Long nextKey);

    VastRequestHeadersBuilder withMaxKeys(long maxKeys);

    VastRequestHeadersBuilder withContentLength(int length);

    VastRequestHeadersBuilder withExactMatch(String schemaNameWithoutBucket);

    VastRequestHeadersBuilder withEndUser(String user);

    VastRequestHeadersBuilder withTraceToken(VastTraceToken traceToken);

    VastRequestHeadersBuilder withSorting(List<Integer> sortedColumns);

    VastRequestHeadersBuilder withNonAcid();
}
