/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.queryengine;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

class RpcError
{
    @JacksonXmlProperty(localName = "Code") public String code;
    @JacksonXmlProperty(localName = "Message") public String message;
    @JacksonXmlProperty(localName = "Resource") public String resource;
    @JacksonXmlProperty(localName = "RequestId") public String requestId;

    @Override
    public String toString()
    {
        return "RpcError{" + "code='" + code + '\'' + ", message='" + message + '\'' + ", resource='" + resource + '\'' + ", requestId='" + requestId + '\'' + '}';
    }
}
