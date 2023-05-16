/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.importdata;

import com.vastdata.client.VastResponse;
import com.vastdata.client.error.ImportDataFailure;
import com.vastdata.client.tx.VastTraceToken;
import io.airlift.http.client.HttpStatus;
import io.airlift.log.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Function;

public final class ImportDataResultHandler
{
    private static final Logger LOG = Logger.get(ImportDataResultHandler.class);

    private static final Function<ImportDataResult, Optional<ImportDataFailure>> RESULT_VERIFIER = result -> result.getFailCount() > 0 ?
            Optional.of(ImportDataFailure.fromSuccessfulRequest(result))
            : Optional.empty();

    private ImportDataResultHandler() {}

    static boolean handleResponse(VastResponse vastResponse, ImportDataResult procedureResult, VastTraceToken traceToken)
            throws ImportDataFailure
    {
        int status = vastResponse.getStatus();
        LOG.debug("ImportData(%s): procedure ended: %s", traceToken, vastResponse);
        byte[] responseBytes = vastResponse.getBytes();
        LOG.debug(new String(responseBytes, StandardCharsets.UTF_8));
        if (status == HttpStatus.OK.code()) {
            handleSuccess(procedureResult);
            return true;
        }
        else if (status == HttpStatus.SERVICE_UNAVAILABLE.code()) {
            LOG.warn("ImportData(%s): Service unavailable during execution: %s", traceToken, vastResponse);
            return false;
        }
        else {
            LOG.error("ImportData(%s): procedure failed with status %s", traceToken, status);
            throw ImportDataFailure.fromFailedRequest(status);
        }
    }

    private static void handleSuccess(ImportDataResult procedureResult)
            throws ImportDataFailure
    {
        Optional<ImportDataFailure> failure = RESULT_VERIFIER.apply(procedureResult);
        if (failure.isPresent()) {
            throw failure.get();
        }
    }
}
