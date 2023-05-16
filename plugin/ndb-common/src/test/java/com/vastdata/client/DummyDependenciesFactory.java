/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import java.util.function.Predicate;

public class DummyDependenciesFactory
        implements VastDependenciesFactory
{
    private VastConfig vastConfig;

    DummyDependenciesFactory(VastConfig vastConfig)
    {
        this.vastConfig = vastConfig;
    }

    private final ValidSchemaNamePredicate schemaNamePredicate = new ValidSchemaNamePredicate();

    @Override
    public Predicate<String> getSchemaNameValidator()
    {
        return schemaNamePredicate;
    }

    @Override
    public VastRequestHeadersBuilder getHeadersFactory()
    {
        return new CommonRequestHeadersBuilder(() -> "DUMMY-" + vastConfig.getEngineVersion());
    }
}
