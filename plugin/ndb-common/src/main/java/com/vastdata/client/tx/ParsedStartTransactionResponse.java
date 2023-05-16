/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx;

public class ParsedStartTransactionResponse
{
    private final long id;

    ParsedStartTransactionResponse(long id)
    {
        this.id = id;
    }

    public long getId()
    {
        return id;
    }
}
