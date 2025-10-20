/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx;

import java.util.Optional;

import static java.lang.String.format;

public class VastTraceToken
{
    private final Optional<String> userTraceToken;
    private final long transactionID;
    private final int queryID;

    public VastTraceToken(Optional<String> userTraceToken, long transactionID, int queryID)
    {
        this.userTraceToken = userTraceToken;
        this.transactionID = transactionID;
        this.queryID = queryID;
    }

    @Override
    public String toString()
    {
        String prefix = userTraceToken.isPresent() ? format("%s:", userTraceToken.get()) : "";
        return format("%s%s:%s", prefix, transactionID, queryID);
    }
}
