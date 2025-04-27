/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.internal.SignerConstants;
import com.amazonaws.http.HttpMethodName;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.log.Logger;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

public class VastRequestBuilder
{
    private static final Logger LOG = Logger.get(VastRequestBuilder.class);
    static final Optional<Map<String, String>> EMPTY_KV_PARAMS = Optional.of(ImmutableMap.of());
    private static final String SERVICE = "s3";

    private final DefaultRequest<?> awsRequest;
    private final Request.Builder builder;
    private final VastConfig config;
    private Optional<Date> date;
    private Optional<byte[]> body;

    public VastRequestBuilder(VastConfig config, HttpMethodName method, String path)
    {
        this(config.getEndpoint(), config, method, path, EMPTY_KV_PARAMS, Collections.emptyMap());
    }

    public VastRequestBuilder(VastConfig config, HttpMethodName method, String path, Map<String, String> parameters)
    {
        this(config.getEndpoint(), config, method, path, EMPTY_KV_PARAMS, parameters);
    }

    public VastRequestBuilder(VastConfig config, HttpMethodName method, String path, Optional<Map<String, String>> keyValueParams, Map<String, String> parameters)
    {
        this(config.getEndpoint(), config, method, path, keyValueParams, parameters);
    }

    public VastRequestBuilder(URI endpoint, VastConfig config, HttpMethodName method, String path, Map<String, String> parameters)
    {
        this(endpoint, config, method, path, EMPTY_KV_PARAMS, parameters);
    }

    public VastRequestBuilder(URI endpoint, VastConfig config, HttpMethodName method, String path, Optional<Map<String, String>> keyValueParams, Map<String, String> parameters)
    {
        // will be signed
        this.awsRequest = new DefaultRequest<>(SERVICE);
        this.awsRequest.setEndpoint(endpoint);
        this.awsRequest.addHeader(SignerConstants.X_AMZ_CONTENT_SHA256, "required");
        this.awsRequest.setHttpMethod(method);
        this.awsRequest.setResourcePath(path);
        parameters.forEach(this.awsRequest::addParameter);
        this.date = Optional.empty();
        this.config = config;
        this.body = Optional.empty();

        // prepare the actual HttpRequest to be sent
        this.builder = new Request.Builder().setMethod(awsRequest.getHttpMethod().name());
        HttpUriBuilder uriBuilder = HttpUriBuilder.uriBuilderFrom(awsRequest.getEndpoint()).appendPath(awsRequest.getResourcePath());
        this.awsRequest.getParameters().forEach((k, v) -> {
            if (Iterables.isEmpty(v) || Strings.isNullOrEmpty(Iterables.getOnlyElement(v))) {
                uriBuilder.addParameter(k);
            }
            else {
                uriBuilder.addParameter(k, v);
            }
        });
        keyValueParams.ifPresent(params -> params.forEach((k, v) -> {
            this.awsRequest.addParameter(k, v);
            uriBuilder.addParameter(k, v);
        }));
        this.builder.setUri(uriBuilder.build());
    }

    // Add "x-tabular" header (will NOT be signed)
    public VastRequestBuilder addHeader(String name, String value)
    {
        this.builder.addHeader(name, value);
        return this;
    }

    // Add "x-tabular" headers (will NOT be signed)
    public VastRequestBuilder addHeaders(Multimap<String, String> headers)
    {
        this.builder.addHeaders(headers);
        return this;
    }

    public VastRequestBuilder setBody(Optional<byte[]> body)
    {
        this.body = body;
        return this;
    }

    public VastRequestBuilder setDate(Date date)
    {
        this.date = Optional.of(date);
        return this;
    }

    public Request build()
    {
        body.ifPresent(value -> {
            awsRequest.setContent(new ByteArrayInputStream(value));
            builder.setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(value));
        });
        AWS4Signer signer = new AWS4Signer();
        date.ifPresent(signer::setOverrideDate);
        signer.setServiceName(SERVICE);
        signer.setRegionName(config.getRegion());
        AWSCredentials credentials = new BasicAWSCredentials(config.getAccessKeyId(), config.getSecretAccessKey());
        signer.sign(awsRequest, credentials); // updates `awsRequest` headers

        builder.addHeaders(ImmutableMultimap.copyOf(awsRequest.getHeaders().entrySet()));

        Request request = builder.build();
        LOG.debug("request: %s", request);
        return request;
    }
}
