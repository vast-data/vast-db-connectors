/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.tx;

import com.vastdata.client.tx.ParsedStartTransactionResponse;
import com.vastdata.client.tx.VastTransactionInstantiator;

public class VastTransactionHandleFactory
        implements VastTransactionInstantiator<VastTransactionHandle>
{
    @Override
    public VastTransactionHandle apply(ParsedStartTransactionResponse parsedStartTransactionResponse)
    {
        return new VastTransactionHandle(parsedStartTransactionResponse.getId());
    }
}
