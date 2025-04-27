package com.vastdata.vdb.sdk;

import com.vastdata.client.VastConfig;

import java.net.URI;

public class VastSdkConfig
{
    private final VastConfig vastConfig;

    public VastSdkConfig(URI endpoint,
            String dataEndpoints,
            String awsAccessKeyId,
            String awsSecretAccessKey)
    {
        this.vastConfig = new VastConfig()
                .setEndpoint(endpoint)
                .setDataEndpoints(dataEndpoints)
                .setAccessKeyId(awsAccessKeyId)
                .setSecretAccessKey(awsSecretAccessKey);
    }

    public VastConfig getVastConfig()
    {
        return this.vastConfig;
    }
}
