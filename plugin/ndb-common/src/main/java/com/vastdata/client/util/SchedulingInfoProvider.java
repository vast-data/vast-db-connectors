/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.util;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTraceToken;

import java.util.Optional;

public final class SchedulingInfoProvider
{
    private SchedulingInfoProvider()
    {
    }

    public static VastSchedulingInfo getVastSchedulingInfo(String schemaName,
            String tableName, SimpleVastTransaction tx, VastClient vastClient,
            String endUser)
    {
        VastTraceToken traceToken = tx != null ?
                tx.generateTraceToken(Optional.empty()) :
                null;
        return vastClient.getSchedulingInfo(tx, traceToken, schemaName,
                tableName, endUser);
    }
}
