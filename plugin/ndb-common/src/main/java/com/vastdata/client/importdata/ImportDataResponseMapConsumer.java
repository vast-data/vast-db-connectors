/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.importdata;

import com.vastdata.client.schema.VastPayloadSerializer;
import io.airlift.log.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.vastdata.client.error.VastExceptionFactory.serverInvalidResponseError;
import static java.util.Objects.nonNull;

public class ImportDataResponseMapConsumer
        implements Consumer<Map>
{
    private final VastPayloadSerializer<Map> mapToString = VastPayloadSerializer.getInstanceForMap();
    private static final Logger LOG = Logger.get(ImportDataResponseMapConsumer.class);
    private final AtomicInteger failedCtr = new AtomicInteger(0);
    private final AtomicInteger successCtr = new AtomicInteger(0);
    private final AtomicReference<String> firstErrorDetails = new AtomicReference<>();

    @Override
    public void accept(Map map)
    {
        Consumer<Map> mapConsumer = v -> {
            Optional<byte[]> responseMapBytes = mapToString.apply(v);
            Object res = v.get("res");
            if (res == null) {
                throw serverInvalidResponseError("Missing result field");
            }
            if (res.equals("Success")) {
                if (responseMapBytes.isPresent()) {
                    byte[] r = responseMapBytes.get();
                    LOG.info("Import data is successful for file: %s", new String(r, StandardCharsets.UTF_8));
                }
                else {
                    LOG.info("Import data is successful for file name: %s", v.get("object_name"));
                }
//                responseMapBytes.ifPresentOrElse(
//                        r -> LOG.info("Import data is successful for file: %s", new String(r, StandardCharsets.UTF_8)),
//                        () -> LOG.info("Import data is successful for file name: %s", v.get("object_name")));
                successCtr.incrementAndGet();
            }
            else if (res.equals("TabularInProgress")) {
                if (responseMapBytes.isPresent()) {
                    byte[] r = responseMapBytes.get();
                    LOG.info("Import data is processing file: %s", new String(r, StandardCharsets.UTF_8));
                }
                else {
                    LOG.info("Import data is processing file name: %s", v.get("object_name"));
                }
//                responseMapBytes.ifPresentOrElse(
//                        r -> LOG.info("Import data is processing file: %s", new String(r, StandardCharsets.UTF_8)),
//                        () -> LOG.info("Import data is processing file name: %s", v.get("object_name")));
            }
            else {
                if (responseMapBytes.isPresent()) {
                    byte[] r = responseMapBytes.get();
                    LOG.info("Import data failed for file: %s", new String(r, StandardCharsets.UTF_8));
                }
                else {
                    LOG.info("Import data failed for file name: %s", v.get("object_name"));
                }
//                responseMapBytes.ifPresentOrElse(
//                        r -> LOG.error("Import data failed for file: %s", new String(r, StandardCharsets.UTF_8)),
//                        () -> LOG.error("Import data failed for file name: %s", v.get("object_name")));
                failedCtr.incrementAndGet();
                firstErrorDetails.getAndUpdate(prev -> nonNull(prev) ? prev : v.toString());
            }
        };
        mapConsumer.accept(map);
    }

    public ImportDataResult getResult()
    {
        return new ImportDataResult(successCtr.get(), failedCtr.get(), Optional.ofNullable(firstErrorDetails.get()));
    }
}
