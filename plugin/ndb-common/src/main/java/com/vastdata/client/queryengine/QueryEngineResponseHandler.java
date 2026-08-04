/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.queryengine;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.vastdata.client.VastResponseHandler;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;

public class QueryEngineResponseHandler<T>
        implements ResponseHandler<T, Exception>
{
    private static final Logger LOG = Logger.get(
            QueryEngineResponseHandler.class);

    public T handleException(Request request, Exception exception)
    {
        VastResponseHandler.internalHandleException(request, exception);
        throw new RuntimeException(exception);
    }

    @Override
    public T handle(Request request, Response response)
            throws Exception
    {
        validateResponse(request, response);
        return internalHandle(request, response);
    }

    protected T internalHandle(Request request, Response response)
            throws Exception
    {
        return null; // do nothing by default
    }

    private void validateResponse(Request request, Response response)
    {
        if (response.getStatusCode() != 200 && response.getStatusCode() != 204) {
            String errorMessage = null;
            if (response.getHeader("Content-Type") != null && response
                    .getHeader("Content-Type")
                    .contains("application/xml")) {
                try {
                    XmlMapper mapper = new XmlMapper();
                    errorMessage = mapper
                            .readValue(response.getInputStream(),
                                    RpcError.class)
                            .toString();
                }
                catch (Exception e) {
                    LOG.debug("unable to read response body for request %s: %s",
                            request.getUri(), e.getMessage());
                }
            }
            if (errorMessage == null) {
                errorMessage = VastResponseHandler.getRequestExceptionTitle(
                        request, "Request failed with status code %s");
            }
            throw new UncheckedIOException(new IOException(errorMessage));
        }
    }
}
