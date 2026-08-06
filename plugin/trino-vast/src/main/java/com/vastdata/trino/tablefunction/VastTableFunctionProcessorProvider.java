/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.tablefunction;

import com.vastdata.client.VastConfig;
import com.vastdata.client.queryengine.VastQueryEngineClient;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

public class VastTableFunctionProcessorProvider
        implements TableFunctionProcessorProvider
{
    private final VastConfig vastConfig;
    private final VastQueryEngineClient vastClient;
    private final TypeManager typeManager;

    public VastTableFunctionProcessorProvider(VastConfig vastConfig,
                                              VastQueryEngineClient vastClient,
                                              TypeManager typeManager)
    {
        this.vastConfig = requireNonNull(vastConfig);
        this.vastClient = requireNonNull(vastClient);
        this.typeManager = requireNonNull(typeManager);
    }

    @Override
    public TableFunctionSplitProcessor getSplitProcessor(ConnectorSession session,
                                                         ConnectorTableFunctionHandle handle,
                                                         ConnectorSplit split)
    {
        return new VastTableFunctionSplitProcessor(vastConfig, vastClient,
                typeManager, session, split);
    }
}
