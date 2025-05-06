/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemAccessControlFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class VastSystemAccessControlFactory implements SystemAccessControlFactory
{
    private static final Logger LOG = Logger.get(VastSystemAccessControlFactory.class);

    @Override
    public String getName()
    {
        return "vast-access-control";
    }

    @Override
    public SystemAccessControl create(final Map<String, String> config)
    {
        requireNonNull(config, "requiredConfig is null");
        // Preconditions.checkArgument(config.isEmpty(), "This access controller does not support any configuration properties");

        // A plugin is not required to use Guice; it is just very convenient
        final Bootstrap app = new Bootstrap(
                new NodeModule(),
                new JsonModule(),
                new VastModule());

        final Injector injector = app
                .doNotInitializeLogging()
                .setOptionalConfigurationProperties(config)
                .setRequiredConfigurationProperties(Map.of())
                .initialize();

        return injector.getInstance(VastSecurityAccessControl.class);
    }
}
