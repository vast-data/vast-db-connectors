/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx;

import com.vastdata.client.RequestsHeaders;
import com.vastdata.client.VastResponse;
import com.vastdata.client.error.VastExceptionFactory;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpStatus;
import io.airlift.log.Logger;

import java.util.Collection;
import java.util.function.Function;

import static com.vastdata.client.error.VastExceptionFactory.serverException;
import static java.util.Objects.requireNonNull;

class VastTransactionResponseParser
        implements Function<VastResponse, ParsedStartTransactionResponse>
{
    private static final Logger LOG = Logger.get(VastTransactionResponseParser.class);

    @Override
    public ParsedStartTransactionResponse apply(VastResponse vastResponse)
    {
        requireNonNull(vastResponse);
        int status = vastResponse.getStatus();
        if (status == HttpStatus.OK.code()) {
            Multimap<HeaderName, String> headers = vastResponse.getHeaders();
            long txid = extractTxIdFromHeaders(headers);
            return new ParsedStartTransactionResponse(txid);
        }
        else {
            LOG.error("Start transaction failed: %s", vastResponse);
            throw VastExceptionFactory.toRuntime(VastExceptionFactory.serverException("Failed starting transaction"));
        }
    }

    protected long extractTxIdFromHeaders(Multimap<HeaderName, String> headers)
    {
        Collection<String> transIDHeader = headers.get(HeaderName.of(RequestsHeaders.TABULAR_TRANSACTION_ID.getHeaderName()));
        String id = Iterables.getOnlyElement(transIDHeader);
        return Long.parseUnsignedLong(id);
    }
}
