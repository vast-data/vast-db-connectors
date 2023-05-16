/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver.handle;

import com.sun.net.httpserver.HttpExchange;
import com.vastdata.client.ParsedURL;
import com.vastdata.mockserver.MockMapSchema;
import io.airlift.log.Logger;

import java.net.URI;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

public class PutHandler
        extends AbstractRequestConsumer
{
    private static final Logger LOG = Logger.get(PutHandler.class);

    public PutHandler(Map<String, Set<MockMapSchema>> schema, Set<String> openTransactions)
    {
        super(schema, openTransactions);
    }

    @Override
    protected void handle(HttpExchange he)
            throws Exception
    {
        URI requestURI = he.getRequestURI();
        LOG.info(format("PUT %s", requestURI));
        ParsedURL of = ParsedURL.of(requestURI.getPath());
        if (of.isBaseUrl()) {
            removeTransaction(he);
        }
        else {
            respondError(he);
        }
    }
}
