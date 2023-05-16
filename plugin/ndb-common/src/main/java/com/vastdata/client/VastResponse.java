/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.google.common.collect.Multimap;
import io.airlift.http.client.HeaderName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class VastResponse
{
    private final int status;
    private final Multimap<HeaderName, String> headers;
    private final byte[] contents;

    public VastResponse(int status, Multimap<HeaderName, String> headers, byte[] contents)
    {
        this.status = status;
        this.headers = headers;
        this.contents = contents;
    }

    public int getStatus()
    {
        return status;
    }

    public Multimap<HeaderName, String> getHeaders()
    {
        return headers;
    }

    public byte[] getBytes()
    {
        return contents;
    }

    public ByteBuffer getByteBuffer()
    {
        return ByteBuffer.wrap(contents);
    }

    public Optional<String> getErrorMessage()
    {
        if (getStatus() != 200) {
            return Optional.ofNullable(parse());
        }
        else {
            return Optional.empty();
        }
    }

    private String parse()
    {
        if (this.contents == null || this.contents.length == 0) {
            return null;
        }
        else {
            try (InputStream is = new ByteArrayInputStream(contents)) {
                return new VastXmlResponsesSaxParser().parseError(is);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("status", status)
                .add("headers", headers)
                .toString();
    }
}
