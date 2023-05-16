/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.transform.Unmarshallers;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.http.client.Response;

import java.io.InputStream;
import java.util.Set;
import java.util.stream.Collectors;

public class ListBucketsResponseHandler
        extends VastResponseHandler
{
    @Override
    public byte[] getBytes(Response response)
            throws Exception
    {
        Set<String> collect = parseBytes(response.getInputStream());
        return new ObjectMapper().writeValueAsBytes(collect);
    }

    protected Set<String> parseBytes(InputStream inputStream)
            throws Exception
    {
        return new Unmarshallers.ListBucketsUnmarshaller().unmarshall(inputStream).stream().map(Bucket::getName).collect(Collectors.toSet());
    }
}
