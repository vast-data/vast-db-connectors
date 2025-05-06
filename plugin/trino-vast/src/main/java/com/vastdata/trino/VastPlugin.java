/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.security.SystemAccessControlFactory;

public class VastPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new VastConnectorFactory());
    }

    @Override
    public Iterable<SystemAccessControlFactory> getSystemAccessControlFactories()
    {
        return ImmutableList.of(new VastSystemAccessControlFactory());
    }
}
