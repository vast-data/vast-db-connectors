/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.annotations.VisibleForTesting;
import com.vastdata.client.CommonRequestHeadersBuilder;
import com.vastdata.client.ValidSchemaNamePredicate;
import com.vastdata.client.VastDependenciesFactory;
import com.vastdata.client.VastRequestHeadersBuilder;
import com.vastdata.client.VastVersion;
import io.trino.spi.connector.Connector;

import java.util.function.Predicate;

public class VastTrinoDependenciesFactory
        implements VastDependenciesFactory
{
    @VisibleForTesting protected static final String VAST_TRINO_CLIENT_TAG = "VastTrinoPlugin-" + VastVersion.SYS_VERSION;
    private final Predicate<String> schemaNamePredicate = new ValidSchemaNamePredicate();

    @Override
    public Predicate<String> getSchemaNameValidator()
    {
        return schemaNamePredicate;
    }

    @Override
    public VastRequestHeadersBuilder getHeadersFactory()
    {
        String trinoVersion = Connector.class.getPackage().getImplementationVersion();
        return new CommonRequestHeadersBuilder(() -> VAST_TRINO_CLIENT_TAG + "-trino-" + trinoVersion);
    }

    public static final String getVastTrinoVersionedClientTag() {
        String trinoVersion = Connector.class.getPackage().getImplementationVersion();
        return "trino-" + trinoVersion;
    }
}
