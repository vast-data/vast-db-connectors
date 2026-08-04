/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.inject.Inject;
import com.vastdata.client.VastClient;
import com.vastdata.client.queryengine.VastQueryEngineClient;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.trino.metrics.SplitSourceMetrics;
import com.vastdata.trino.statistics.VastStatisticsManager;
import com.vastdata.trino.tablefunction.VastConnectorTableFunctionHandle;
import com.vastdata.trino.tablefunction.VastFunctionSplitSource;
import com.vastdata.trino.tx.VastTransactionHandle;
import com.vastdata.trino.tx.VastTrinoTransactionHandleManager;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import static java.util.Objects.requireNonNull;

public class VastSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;
    private final VastClient client;
    private final VastPageSourceProvider vastPageSourceProvider;
    private final VastQueryEngineClient vastQueryEngineClient;
    private final VastTrinoTransactionHandleManager vastTrinoTransactionHandleManager;
    private final VastStatisticsManager statisticsManager;
    private final SplitSourceMetrics splitSourceMetrics;

    @Inject
    public VastSplitManager(NodeManager nodeManager,
                            VastQueryEngineClient vastQueryEngineClient,
                            VastClient client,
                            VastPageSourceProvider vastPageSourceProvider,
                            VastTrinoTransactionHandleManager vastTrinoTransactionHandleManager,
                            VastStatisticsManager statisticsManager,
                            SplitSourceMetrics splitSourceMetrics)
    {
        this.nodeManager = requireNonNull(nodeManager);
        this.client = requireNonNull(client);
        this.vastPageSourceProvider = requireNonNull(vastPageSourceProvider);
        this.vastQueryEngineClient = requireNonNull(vastQueryEngineClient);
        this.vastTrinoTransactionHandleManager = requireNonNull(
                vastTrinoTransactionHandleManager);
        this.statisticsManager = requireNonNull(statisticsManager);
        this.splitSourceMetrics = requireNonNull(splitSourceMetrics);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
                                          ConnectorSession session,
                                          ConnectorTableHandle connectorTableHandle,
                                          DynamicFilter dynamicFilter,
                                          Constraint constraint)
    {
        VastTableHandle vastTableHandle = (VastTableHandle) connectorTableHandle;
        if (vastTableHandle.getPartitionColumns().isPresent()) {
            return new VastPartitionedSplitSource(nodeManager, client,
                    statisticsManager, vastPageSourceProvider,
                    vastTrinoTransactionHandleManager, splitSourceMetrics,
                    (VastTransaction) transaction, session,
                    (VastTableHandle) connectorTableHandle, dynamicFilter);
        }
        else {
            return new VastEstimatingRowsSplitSource(nodeManager,
                    vastPageSourceProvider, client, statisticsManager,
                    splitSourceMetrics, (VastTransaction) transaction, session,
                    (VastTableHandle) connectorTableHandle, dynamicFilter);
        }
    }

    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
                                          ConnectorSession session,
                                          ConnectorTableFunctionHandle functionHandle)
    {
        return new VastFunctionSplitSource(vastQueryEngineClient,
                vastTrinoTransactionHandleManager,
                (VastTransactionHandle) transaction,
                (VastConnectorTableFunctionHandle) functionHandle, session);
    }
}
