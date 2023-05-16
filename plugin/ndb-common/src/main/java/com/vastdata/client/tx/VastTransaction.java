/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.tx;

import java.util.Optional;

public interface VastTransaction
{
    long getId();

    boolean isReadOnly();

    VastTraceToken generateTraceToken(Optional<String> userTraceToken);
}
