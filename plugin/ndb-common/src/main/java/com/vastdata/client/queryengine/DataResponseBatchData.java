/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.queryengine;

import vastdb.queryengine.protocol.GetDataResponse;

import java.io.InputStream;

public class DataResponseBatchData
{
    private final GetDataResponse dataResponse;
    private final InputStream inputStream;

    public DataResponseBatchData(GetDataResponse dataResponse,
            InputStream inputStream)
    {
        this.dataResponse = dataResponse;
        this.inputStream = inputStream;
    }

    public GetDataResponse getDataResponse()
    {
        return dataResponse;
    }

    public InputStream getInputStream()
    {
        return inputStream;
    }
}
