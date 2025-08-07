/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx;

import java.util.function.Function;

public interface VastTransactionInstantiator<T>
        extends Function<ParsedStartTransactionResponse, T>
{
}
