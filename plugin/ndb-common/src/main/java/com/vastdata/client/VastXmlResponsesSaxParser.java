/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.transform.XmlResponsesSaxParser;
import io.airlift.log.Logger;

import java.io.IOException;
import java.io.InputStream;

public class VastXmlResponsesSaxParser extends XmlResponsesSaxParser
{
    private static final Logger LOG = Logger.get(VastXmlResponsesSaxParser.class);

    public String parseError(InputStream is)
            throws IOException
    {
        VastErrorHandler handler = new VastErrorHandler();
        try {
            parseXmlInputStream(handler, is);
        }
        catch (SdkClientException internalRuntimeException) {
            LOG.error(internalRuntimeException, "Failed parsing xml");
        }
        return handler.getMessage();
    }
}
