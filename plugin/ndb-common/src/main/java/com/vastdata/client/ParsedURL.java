/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.google.common.base.Strings;
import com.vastdata.client.error.VastUserException;

import java.util.Arrays;
import java.util.StringJoiner;

import static com.vastdata.client.VerifyParam.verify;
import static com.vastdata.client.error.VastExceptionFactory.userException;
import static java.lang.String.format;

public class ParsedURL
{
    public static String PATH_SEPERATOR = "/";
    private final String[] parts;

    private ParsedURL(String[] parts)
    {
        this.parts = parts;
    }

    public static String compose(String[] parts)
    {
        if (parts.length == 0) {
            return "";
        }
        StringJoiner joiner = new StringJoiner(PATH_SEPERATOR);
        for (String part : parts) {
            joiner.add(part);
        }
        return joiner.toString();
    }

    public static ParsedURL of(String path)
            throws VastUserException
    {
        verify(!Strings.isNullOrEmpty(path), "Empty URL");
        String[] urlParts = parse(path);
        for (int i = 1; i < urlParts.length; i++) {
            verify(!Strings.isNullOrEmpty(urlParts[i]), format("Empty URL part at index: %s", i));
        }
        return new ParsedURL(urlParts);
    }

    public static String[] parse(String path)
    {
        return removeTrailingSlash(addHeadingSlash(path)).split(PATH_SEPERATOR);
    }

    private static String addHeadingSlash(String url)
    {
        if (!url.startsWith("/")) {
            return "/" + url;
        }
        else {
            return url;
        }
    }

    private static String removeTrailingSlash(String url)
    {
        if (url.endsWith("/")) {
            return url.substring(0, url.lastIndexOf("/"));
        }
        else {
            return url;
        }
    }

    public boolean isBaseUrl()
    {
        return parts.length == 1;
    }

    public boolean isBucketURL()
    {
        return parts.length == 2;
    }

    public boolean isSchemaURL()
    {
        return parts.length == 3;
    }

    public boolean hasTable()
    {
        return parts.length > 3 && !Strings.isNullOrEmpty(parts[3]);
    }

    public String getBucket()
            throws VastUserException
    {
        if (isBaseUrl()) {
            throw userException("url does not contain bucket name");
        }
        else {
            return parts[1];
        }
    }

    public String getSchemaName()
            throws VastUserException
    {
        if (parts.length > 2) {
            StringJoiner joiner = new StringJoiner("/");
            for (int i = 2; i < Math.max(parts.length - 1, 3); i++) {
                joiner.add(parts[i]);
            }
            return joiner.toString();
        }
        throw userException(format("url does not contain schema name: %s", Arrays.asList(parts)));
    }

    public String[] getFullSchemaParts()
    {
        return Arrays.copyOfRange(parts, 1, parts.length - 1);
    }

    public String getFullSchemaPath()
            throws VastUserException
    {
        if (parts.length > 2) {
            StringJoiner joiner = new StringJoiner("/");
            for (int i = 1; i < Math.max(parts.length - 1, 3); i++) {
                joiner.add(parts[i]);
            }
            return joiner.toString();
        }
        throw userException(format("url does not contain schema name: %s", Arrays.asList(parts)));
    }

    public String getTableName()
            throws VastUserException
    {
        if (!hasTable()) {
            throw userException(format("url does not contain table name: %s", Arrays.asList(parts)));
        }
        return parts[parts.length - 1];
    }
}
