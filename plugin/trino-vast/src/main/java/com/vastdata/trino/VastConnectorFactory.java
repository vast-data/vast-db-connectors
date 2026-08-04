/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class VastConnectorFactory
        implements ConnectorFactory
{
    private static final Logger LOG = Logger.get(VastConnectorFactory.class);

    private final VastModule vastModule;

    public VastConnectorFactory(VastModule vastModule)
    {
        this.vastModule = vastModule;
    }

    public VastConnectorFactory()
    {
        this(VastModule.builder(false).build());
    }

    @Override
    public String getName()
    {
        return "vast";
    }

    @Override
    public Connector create(String catalogName,
                            Map<String, String> requiredConfig,
                            ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        LOG.info("catalogName=%s, requiredConfig=%s, context=%s", catalogName,
                requiredConfig, context);

        // A plugin is not required to use Guice; it is just very convenient
        Bootstrap app = new Bootstrap(new NodeModule(), new MBeanServerModule(),
                new MBeanModule(), new JsonModule(),
                new TypeDeserializerModule(), binder ->
        {
            binder.bind(NodeManager.class).toInstance(context.getNodeManager());
            binder.bind(TypeManager.class).toInstance(context.getTypeManager());
        }, this.vastModule);

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();
        return injector.getInstance(VastConnector.class);
    }
}
