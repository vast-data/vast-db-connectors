/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.vastdata.client.tx.VastTraceToken;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.log.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

import static com.vastdata.client.error.VastExceptionFactory.ioException;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static java.lang.String.format;

public class QueryDataResponseHandler
        extends VastResponseHandler
{
    private static final Logger LOG = Logger.get(QueryDataResponseHandler.class);

    private final Consumer<InputStream> parser;
    private final VastTraceToken traceToken;
    private final long startTimeNanos;

    public QueryDataResponseHandler(Consumer<InputStream> parser, VastTraceToken traceToken)
    {
        this.parser = parser;
        this.traceToken = traceToken;
        this.startTimeNanos = System.nanoTime();
    }

    @Override
    protected String getRequestExceptionTitle(Request request)
    {
        return format("QueryData(%s) failed with exception: %s", traceToken, request);
    }

    @Override
    public VastResponse handle(Request request, Response response)
    {
        LOG.debug("QueryData(%s) response: %s", traceToken, response);
        if (response.getStatusCode() != 200) {
            LOG.error("QueryData(%s) request failed: %s", traceToken, response);
            // VAST may respond with HTTP 503 (if the cluster is busy), so we can retry later
            return new VastResponse(response.getStatusCode(), response.getHeaders(), getRequestPayloadBytes(request, response), request.getUri());
        }
        try {
            // we MUST read all contents before this method exits, otherwise the connection will be closed
            parser.accept(response.getInputStream());
            long durationNanos = System.nanoTime() - startTimeNanos;
            LOG.debug("QueryData(%s) finished parsing, read %d bytes, took %.3f ms",
                    traceToken, response.getBytesRead(), durationNanos / 1e6);
        }
        catch (IOException e) {
            throw toRuntime(ioException(format("QueryData(%s) failed to parse response: " + request, traceToken), e));
        }
        // query finished successfully
        return new VastResponse(response.getStatusCode(), response.getHeaders(), null, request.getUri());
    }
}
