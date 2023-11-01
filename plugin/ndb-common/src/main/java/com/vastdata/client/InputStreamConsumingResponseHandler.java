/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.log.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

import static com.vastdata.client.error.VastExceptionFactory.ioException;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;

public class InputStreamConsumingResponseHandler
        extends VastResponseHandler
{
    private static final Logger LOG = Logger.get(InputStreamConsumingResponseHandler.class);
    private final Consumer<InputStream> inputStreamConsumer;

    public InputStreamConsumingResponseHandler(Consumer<InputStream> inputStreamConsumer)
    {
        this.inputStreamConsumer = inputStreamConsumer;
    }

    @Override
    public VastResponse handle(Request request, Response response)
    {
        if (response.getStatusCode() != HttpStatus.OK.code()) {
            return super.handle(request, response);
        }
        else {
            LOG.debug("response: %s", response);
            try (InputStream inputStream = response.getInputStream()) {
                inputStreamConsumer.accept(inputStream);
                return new VastResponse(response.getStatusCode(), response.getHeaders(), new byte[0], request.getUri());
            }
            catch (IOException e) {
                throw toRuntime(ioException("Failed reading input stream", e));
            }
        }
    }
}
