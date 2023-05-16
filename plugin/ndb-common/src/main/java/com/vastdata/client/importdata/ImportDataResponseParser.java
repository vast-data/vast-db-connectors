/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.importdata;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;

import static com.vastdata.client.error.VastExceptionFactory.ioException;
import static com.vastdata.client.error.VastExceptionFactory.serializationException;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;

public class ImportDataResponseParser
        implements Consumer<InputStream>
{
    private static final Logger LOG = Logger.get(ImportDataResponseParser.class);

    private final Consumer<Map> mapConsumer;

    public ImportDataResponseParser(Consumer<Map> mapConsumer)
    {
        this.mapConsumer = mapConsumer;
    }

    private void iterateJsonsFromStream(InputStream is, Consumer<Map> mapConsumer)
    {
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory jsonFactory = new JsonFactory();
        try (Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
            mapper.readValues(jsonFactory.createParser(reader), Map.class).forEachRemaining(mapConsumer);
        }
        catch (JsonParseException e) {
            LOG.error(e, "Failed parsing json response");
            throw toRuntime(serializationException("Failed parsing json response", e));
        }
        catch (IOException e) {
            LOG.error(e, "Failed parsing json response");
            throw toRuntime(ioException("Failed parsing json response", e));
        }
    }

    @Override
    public void accept(InputStream inputStream)
    {
        iterateJsonsFromStream(inputStream, mapConsumer);
    }
}
