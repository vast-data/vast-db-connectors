/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx;

public class VastTransactionFactory
        implements VastTransactionInstantiator<SimpleVastTransaction>
{
    @Override
    public SimpleVastTransaction apply(ParsedStartTransactionResponse parsedStartTransactionResponse)
    {
        return new SimpleVastTransaction(parsedStartTransactionResponse.getId());
    }
}
