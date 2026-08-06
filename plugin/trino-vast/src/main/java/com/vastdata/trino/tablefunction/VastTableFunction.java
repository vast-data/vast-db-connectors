/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.tablefunction;

import com.google.inject.Inject;
import com.vastdata.client.VastConfig;
import com.vastdata.client.queryengine.VastQueryEngineClient;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProviderFactory;
import io.trino.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

public class VastTableFunction
        implements FunctionProvider
{
    private final VastConfig vastConfig;
    private final VastQueryEngineClient vastClient;
    private final TypeManager typeManager;

    @Inject
    public VastTableFunction(VastConfig vastConfig,
                             VastQueryEngineClient vastClient,
                             TypeManager typeManager)
    {
        this.vastConfig = requireNonNull(vastConfig);
        this.vastClient = requireNonNull(vastClient);
        this.typeManager = requireNonNull(typeManager);
    }

    @Override
    public TableFunctionProcessorProviderFactory getTableFunctionProcessorProviderFactory(
            ConnectorTableFunctionHandle functionHandle)
    {
        return new VastTableFunctionProcessorProviderFactory(vastConfig,
                vastClient, typeManager);
    }
}
