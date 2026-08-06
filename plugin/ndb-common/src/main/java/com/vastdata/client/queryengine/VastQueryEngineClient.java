/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.queryengine;

import com.amazonaws.http.HttpMethodName;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.vastdata.client.ForVast;
import com.vastdata.client.InputStreamToByteArrayReader;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastDependenciesFactory;
import com.vastdata.client.VastResponseHandler;
import com.vastdata.client.tx.VastTransaction;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.log.Logger;
import vastdb.queryengine.protocol.Cancellation;
import vastdb.queryengine.protocol.FinishDataRequest;
import vastdb.queryengine.protocol.FinishQueryRequest;
import vastdb.queryengine.protocol.GetDataRequest;
import vastdb.queryengine.protocol.GetDataResponse;
import vastdb.queryengine.protocol.GetQueryStatusRequest;
import vastdb.queryengine.protocol.GetQueryStatusResponse;
import vastdb.queryengine.protocol.Offset;
import vastdb.queryengine.protocol.QueryId;
import vastdb.queryengine.protocol.StartQueryRequest;
import vastdb.queryengine.protocol.StartQueryResponse;
import vastdb.queryengine.protocol.Ticket;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.amazonaws.http.HttpMethodName.DELETE;
import static com.amazonaws.http.HttpMethodName.POST;
import static java.util.Objects.requireNonNull;

public class VastQueryEngineClient
{
    public static final String DEFAULT_USERNAME = "internal-user";
    private static final Logger LOG = Logger.get(VastQueryEngineClient.class);
    private static final Pattern TICKET_ENDPOINT_PATTERN = Pattern.compile(
            "ipv4-(\\d+)-(\\d+)-(\\d+)-(\\d+)");
    private static final String EMPTY_PAYLOAD_HASH = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    private final HttpClient httpClient;
    private final VastConfig vastConfig;
    private final VastDependenciesFactory dependenciesFactory;

    @Inject
    public VastQueryEngineClient(@ForVast HttpClient httpClient,
            VastConfig vastConfig, VastDependenciesFactory dependenciesFactory)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.vastConfig = requireNonNull(vastConfig, "vastConfig is null");
        this.dependenciesFactory = requireNonNull(dependenciesFactory,
                "dependenciesFactory is null");
    }

    private static byte[] sha256(byte[] data)
    {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(data);
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }

    private static byte[] hmac256(byte[] message, byte[] secret)
    {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKeySpec = new SecretKeySpec(secret,
                    "HmacSHA256");
            mac.init(secretKeySpec);
            return mac.doFinal(message);
        }
        catch (Exception e) {
            throw new RuntimeException("Error while generating HMAC-SHA256", e);
        }
    }

    private static String toLowercaseHex(byte[] data)
    {
        StringBuilder hexString = new StringBuilder();
        for (byte b : data) {
            hexString.append(String.format("%02x", b));
        }
        return hexString.toString();
    }

    public void finishData(QueryId queryId, Ticket ticket, String reason)
            throws Exception
    {
        Multimap<String, String> headers = dependenciesFactory
                .getHeadersFactory(DEFAULT_USERNAME)
                .build();
        FinishDataRequest builder = FinishDataRequest
                .newBuilder()
                .setTicketId(ticket.getTicketId())
                .setQueryId(queryId)
                .setReason(Cancellation
                        .newBuilder()
                        .setDescription(reason)
                        .build())
                .build();
        Request req = buildRequest(DELETE, "/", "vastdb-data", headers,
                builder.toByteArray());
        httpClient.execute(req, new QueryEngineResponseHandler<Void>());
    }

    public DataResponseBatchData getData(boolean useTicketGlobalEndpoint,
            QueryId queryId, Ticket ticket, Offset offset)
            throws Exception
    {
        Multimap<String, String> headers = dependenciesFactory
                .getHeadersFactory(DEFAULT_USERNAME)
                .build();
        GetDataRequest builder = GetDataRequest
                .newBuilder()
                .setTicketId(ticket.getTicketId())
                .setQueryId(queryId)
                .setOffset(offset)
                .build();

        URI endpointURI = createTicketEndpoint(useTicketGlobalEndpoint, ticket);
        Request req = buildRequest(HttpMethodName.POST, endpointURI, "/",
                "vastdb-data", headers, builder.toByteArray());
        return httpClient.execute(req,
                new QueryEngineResponseHandler<DataResponseBatchData>()
                {
                    @Override
                    public DataResponseBatchData internalHandle(Request request,
                            Response response)
                            throws Exception
                    {
                        validateResponse(request, response);
                        String size = response.getHeader(
                                "tabular-protobuf-size");
                        byte[] getDataBuf = new byte[Integer.parseInt(size)];
                        response
                                .getInputStream()
                                .read(getDataBuf, 0, Integer.parseInt(size));
                        GetDataResponse getDataResponse = GetDataResponse.parseFrom(
                                getDataBuf);
                        InputStreamToByteArrayReader reader = new InputStreamToByteArrayReader();
                        byte[] allBytes = reader.readAllBytes(
                                response.getInputStream());
                        return new DataResponseBatchData(getDataResponse,
                                new ByteArrayInputStream(allBytes));
                    }
                });
    }

    public StartQueryResponse startQuery(VastTransaction tx, String query)
            throws Exception
    {
        Multimap<String, String> headers = dependenciesFactory
                .getHeadersFactory(DEFAULT_USERNAME)
                .build();
        StartQueryRequest startQueryRequest = StartQueryRequest
                .newBuilder()
                .setSql(query)
                .setTxid(tx.getId())
                .build();
        Request req = buildRequest(POST, "/", "vastdb-run-query", headers,
                startQueryRequest.toByteArray());
        return httpClient.execute(req,
                new QueryEngineResponseHandler<StartQueryResponse>()
                {
                    @Override
                    public StartQueryResponse internalHandle(Request request,
                            Response response)
                            throws Exception
                    {
                        return StartQueryResponse.parseFrom(
                                response.getInputStream());
                    }
                });
    }

    public void finishQuery(QueryId queryId, String reason)
            throws Exception
    {
        Multimap<String, String> headers = dependenciesFactory
                .getHeadersFactory(DEFAULT_USERNAME)
                .build();
        Cancellation cancellation = Cancellation
                .newBuilder()
                .setDescription(reason)
                .build();
        FinishQueryRequest startQueryRequest = FinishQueryRequest
                .newBuilder()
                .setQueryId(queryId)
                .setReason(cancellation)
                .build();
        Request req = buildRequest(DELETE, "/", "vastdb-run-query", headers,
                startQueryRequest.toByteArray());
        httpClient.execute(req, new QueryEngineResponseHandler<Void>());
    }

    public GetQueryStatusResponse getQueryStatus(QueryId queryId)
            throws Exception
    {
        Multimap<String, String> headers = dependenciesFactory
                .getHeadersFactory(DEFAULT_USERNAME)
                .build();
        GetQueryStatusRequest statusBuilder = GetQueryStatusRequest
                .newBuilder()
                .setQueryId(queryId)
                .build();
        Request req = buildRequest(POST, "/", "vastdb-query-status", headers,
                statusBuilder.toByteArray());
        return httpClient.execute(req,
                new QueryEngineResponseHandler<GetQueryStatusResponse>()
                {
                    @Override
                    public GetQueryStatusResponse internalHandle(
                            Request request, Response response)
                            throws Exception
                    {
                        return GetQueryStatusResponse.parseFrom(
                                response.getInputStream());
                    }
                });
    }

    private Request buildRequest(HttpMethodName method, String path,
            String command, Multimap<String, String> headers, byte[] body)
    {
        return buildRequest(method, vastConfig.getEndpoint(), path, command,
                headers, body);
    }

    private Request buildRequest(HttpMethodName method, URI endpoint,
            String path, String command, Multimap<String, String> headers,
            byte[] body)
    {
        Instant now = Instant.now();
        String datetimeNow = DateTimeFormatter
                .ofPattern("yyyyMMdd'T'HHmmss'Z'")
                .withZone(ZoneOffset.UTC)
                .format(now);
        String dateNow = DateTimeFormatter
                .ofPattern("yyyyMMdd")
                .withZone(ZoneOffset.UTC)
                .format(now);

        String service = "s3";
        String host = "host";
        String region = "vast";
        String query = command + "=";
        String payloadHash = body == null ?
                EMPTY_PAYLOAD_HASH :
                toLowercaseHex(sha256(body));

        String signedHeaders = "host;x-amz-content-sha256;x-amz-date";

        String canonicalRequest = String.join("\n", method.name(), path, query,
                "host:" + host, "x-amz-content-sha256:" + payloadHash,
                "x-amz-date:" + datetimeNow, "", signedHeaders, payloadHash);
        byte[] canonicalRequestHash = sha256(
                canonicalRequest.getBytes(Charset.defaultCharset()));
        String stringToSign = String.format(
                "AWS4-HMAC-SHA256\n%s\n%s/%s/%s/aws4_request\n%s", datetimeNow,
                dateNow, region, service, toLowercaseHex(canonicalRequestHash));

        String signKey = "AWS4" + vastConfig.getSecretAccessKey();
        byte[] kDate = hmac256(dateNow.getBytes(Charset.defaultCharset()),
                signKey.getBytes(Charset.defaultCharset()));
        byte[] kRegion = hmac256(region.getBytes(Charset.defaultCharset()),
                kDate);
        byte[] kService = hmac256(service.getBytes(Charset.defaultCharset()),
                kRegion);
        byte[] signingKey = hmac256(
                "aws4_request".getBytes(Charset.defaultCharset()), kService);
        byte[] signature = hmac256(
                stringToSign.getBytes(Charset.defaultCharset()), signingKey);
        String signatureStr = toLowercaseHex(signature);

        String authorization = String.format(
                "AWS4-HMAC-SHA256 Credential=%s/%s/%s/%s/aws4_request, SignedHeaders=%s, Signature=%s",
                vastConfig.getAccessKeyId(), dateNow, region, service,
                signedHeaders, signatureStr);

        String tabularClientName = dependenciesFactory.getClientTag();
        HttpUriBuilder uriBuilder = HttpUriBuilder
                .uriBuilderFrom(endpoint)
                .appendPath(path)
                .addParameter(query);
        URI uri = uriBuilder.build();

        Request.Builder builder = Request
                .builder()
                .setUri(uri)
                .setMethod(method.name())
                .setBodyGenerator(
                        StaticBodyGenerator.createStaticBodyGenerator(body))
                .addHeader("Authorization", authorization)
                .addHeader("Host", host)
                .addHeader("tabular-api-version-id", "1")
                .addHeader("tabular-client-name", tabularClientName)
                .addHeader("x-amz-content-sha256", payloadHash)
                .addHeader("x-amz-date", datetimeNow);

        for (Map.Entry<String, String> header : headers.entries()) {
            builder.addHeader(header.getKey(), header.getValue());
        }

        return builder.build();
    }

    private void validateResponse(Request request, Response response)
    {
        if (response.getStatusCode() != 200 && response.getStatusCode() != 204) {
            String errorMessage = VastResponseHandler.getRequestExceptionTitle(
                    request, "Request failed with status code %s");
            LOG.error(errorMessage);
            throw new UncheckedIOException(new IOException(errorMessage));
        }
    }

    private URI createTicketEndpoint(boolean useTicketGlobalEndpoint,
            Ticket ticket)
    {
        if (useTicketGlobalEndpoint) {
            return vastConfig.getEndpoint();
        }
        URI globalEndpointURI = vastConfig.getEndpoint();
        Matcher matcher = TICKET_ENDPOINT_PATTERN.matcher(
                ticket.getEndpoints(0).getUri());
        String ticketEndpoint = ticket.getEndpoints(0).getUri();
        if (matcher.find()) {
            ticketEndpoint = String.format("%s.%s.%s.%s", matcher.group(1),
                    matcher.group(2), matcher.group(3), matcher.group(4));
        }
        return HttpUriBuilder
                .uriBuilder()
                .scheme(globalEndpointURI.getScheme())
                .host(ticketEndpoint)
                .port(globalEndpointURI.getPort())
                .build();
    }
}
