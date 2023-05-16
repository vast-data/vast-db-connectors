/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.vastdata.client.RequestsHeaders;
import io.airlift.http.client.HeaderName;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestVastTransactionResponseParser
{
    @Test
    public void testExtractTxIdFromHeadersGraceful()
    {
        Multimap<HeaderName, String> testHeadersMap = ImmutableMultimap.of(HeaderName.of(RequestsHeaders.TABULAR_TRANSACTION_ID.getHeaderName()), "514026084031791104");
        long parsedTransID = new VastTransactionResponseParser().extractTxIdFromHeaders(testHeadersMap);
        assertEquals(Long.toUnsignedString(parsedTransID), "514026084031791104");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testExtractTxIdFromHeadersInvalid()
    {
        Multimap<HeaderName, String> testHeadersMap = ImmutableMultimap.of(HeaderName.of(RequestsHeaders.TABULAR_TRANSACTION_ID.getHeaderName()), "514026084031791104",
                HeaderName.of(RequestsHeaders.TABULAR_TRANSACTION_ID.getHeaderName()), "514026084031791105");
        new VastTransactionResponseParser().extractTxIdFromHeaders(testHeadersMap);
    }
}
