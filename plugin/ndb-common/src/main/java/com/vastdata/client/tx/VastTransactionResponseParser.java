/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.vastdata.client.RequestsHeaders;
import com.vastdata.client.VastResponse;
import com.vastdata.client.error.VastExceptionFactory;
import com.vastdata.client.error.VastRuntimeException;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpStatus;
import io.airlift.log.Logger;

import java.util.Collection;

import static com.vastdata.client.error.VastExceptionFactory.checkResponseStatus;
import static java.util.Objects.requireNonNull;

public class VastTransactionResponseParser
{
    private static final Logger LOG = Logger.get(
            VastTransactionResponseParser.class);
    private static final VastRuntimeException FAILED_STARTING_TRANSACTION_GENERIC_ERROR = VastExceptionFactory.toRuntime(
            VastExceptionFactory.serverException("Start transaction failed"));

    public long parseStartTransactionResponse(VastResponse vastResponse)
            throws VastRuntimeException
    {
        requireNonNull(vastResponse);
        int status = vastResponse.getStatus();
        if (status == HttpStatus.OK.code()) {
            Multimap<HeaderName, String> headers = vastResponse.getHeaders();
            return extractTxIdFromHeaders(headers);
        }
        else {
            String errMessage = "Failed starting transaction";
            LOG.error("%s: %s", errMessage, vastResponse);
            throw checkResponseStatus(vastResponse, errMessage)
                    .map(VastExceptionFactory::toRuntime)
                    .orElse(FAILED_STARTING_TRANSACTION_GENERIC_ERROR);
        }
    }

    protected long extractTxIdFromHeaders(Multimap<HeaderName, String> headers)
    {
        Collection<String> transIDHeader = headers.get(HeaderName.of(
                RequestsHeaders.TABULAR_TRANSACTION_ID.getHeaderName()));
        String id = Iterables.getOnlyElement(transIDHeader);
        return Long.parseUnsignedLong(id);
    }
}
