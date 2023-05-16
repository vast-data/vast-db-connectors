/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.vastdata.client.error.VastUserException;

import java.util.function.Predicate;

public class ValidSchemaNamePredicate
        implements Predicate<String>
{
    @Override
    public boolean test(String name)
    {
        try {
            ParsedURL parsedURL = ParsedURL.of(name);
            return parsedURL.isSchemaURL() || parsedURL.hasTable();
        }
        catch (VastUserException e) {
            return false;
        }
    }
}
