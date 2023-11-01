/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.inject.Inject;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastVersion;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.trino.statistics.VastStatisticsManager;
import com.vastdata.trino.tx.VastTransactionHandle;
import com.vastdata.trino.tx.VastTrinoTransactionHandleManager;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.vastdata.client.error.VastExceptionFactory.closedTransaction;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

public class VastConnector
        implements Connector
{
    private static final Logger LOG = Logger.get(VastConnector.class);
    private final LifeCycleManager lifeCycleManager;
    private final VastClient client;
    private final VastTrinoTransactionHandleManager transManager;
    private final VastSplitManager splitManager;
    private final VastPageSourceProvider pageSourceProvider;
    private final VastPageSinkProvider pageSinkProvider;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final VastStatisticsManager statisticsManager;

    @Inject
    public VastConnector(
            LifeCycleManager lifeCycleManager,
            VastClient client,
            VastTrinoTransactionHandleManager transManager,
            VastSplitManager splitManager,
            VastPageSourceProvider pageSourceProvider,
            VastPageSinkProvider pageSinkProvider,
            VastStatisticsManager statisticsManager,
            Set<SessionPropertiesProvider> sessionProperties)
    {
        LOG.info("Creating VAST connector: system=%s, hash=%s", VastVersion.SYS_VERSION, VastVersion.HASH);
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.client = requireNonNull(client, "vast client is null");
        this.transManager = requireNonNull(transManager, "vast transaction factory is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.statisticsManager = requireNonNull(statisticsManager, "statisticsManager is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null").stream()
                .flatMap(sessionPropertiesProvider -> sessionPropertiesProvider.getSessionProperties().stream())
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        LOG.debug("Starting transaction");
        return this.transManager.startTransaction(new StartTransactionContext(readOnly, autoCommit));
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        LOG.debug("Committing transaction %s", transactionHandle);
        this.transManager.commit((VastTransactionHandle) transactionHandle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        LOG.debug("Rolling back transaction %s", transactionHandle);
        this.transManager.rollback((VastTransactionHandle) transactionHandle);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        LOG.info("Creating VAST metadata: system=%s, hash=%s, tx=%s", VastVersion.SYS_VERSION, VastVersion.HASH, transactionHandle);
        VastTransactionHandle vastTransHandle = (VastTransactionHandle) transactionHandle;
        if (!this.transManager.isOpen(vastTransHandle)) {
            throw closedTransaction(vastTransHandle);
        }
        return new VastMetadata(client, vastTransHandle, this.statisticsManager);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return List.of(stringProperty(
                "column_stats",
                "Persistent Column Statistics",
                null,
                false));
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return Set.of(new VastSystemTable());
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return Set.of();
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return false;
    }
}
