/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver;

import com.amazonaws.http.HttpMethodName;

import java.util.Objects;

import static java.lang.String.format;

public class HookKey
{
    private final String path;

    private final HttpMethodName method;

    public HookKey(String path, HttpMethodName method)
    {
        this.path = path;
        this.method = method;
    }

    @Override
    public String toString()
    {
        return format("%s:%s", method, path);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HookKey hookKey = (HookKey) o;
        return path.equals(hookKey.path) && method == hookKey.method;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, method);
    }
}
