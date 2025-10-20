/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx;

import com.vastdata.client.schema.StartTransactionContext;

import java.util.function.BiFunction;

public interface VastTransactionInstantiator<T>
        extends BiFunction<StartTransactionContext, ParsedStartTransactionResponse, T>
{
}
