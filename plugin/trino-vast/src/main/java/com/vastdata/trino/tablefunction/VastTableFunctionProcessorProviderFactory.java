/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.tablefunction;

import com.vastdata.client.VastConfig;
import com.vastdata.client.queryengine.VastQueryEngineClient;
import io.airlift.log.Logger;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionProcessorProviderFactory;
import io.trino.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

public class VastTableFunctionProcessorProviderFactory
        implements TableFunctionProcessorProviderFactory
{
    private static final Logger LOG = Logger.get(
            VastTableFunctionProcessorProviderFactory.class);

    private final VastQueryEngineClient vastClient;
    private final VastConfig vastConfig;
    private final TypeManager typeManager;

    public VastTableFunctionProcessorProviderFactory(VastConfig vastConfig,
                                                     VastQueryEngineClient vastClient,
                                                     TypeManager typeManager)
    {
        this.vastConfig = requireNonNull(vastConfig);
        this.vastClient = requireNonNull(vastClient);
        this.typeManager = requireNonNull(typeManager);
    }

    @Override
    public TableFunctionProcessorProvider createTableFunctionProcessorProvider()
    {
        LOG.info("Creating TableFunctionProcessorProvider");
        return new VastTableFunctionProcessorProvider(vastConfig, vastClient,
                typeManager);
    }
}
