/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.vastdata.ShapingLoggerFactory;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastVersion;
import com.vastdata.client.partition.PartitionColumnMetadata;
import com.vastdata.client.queryengine.VastQueryEngineClient;
import com.vastdata.trino.statistics.VastStatisticsManager;
import com.vastdata.trino.tablefunction.VastConnectorTableFunctionHandle;
import com.vastdata.trino.tablefunction.VastTableFunction;
import com.vastdata.trino.tx.VastTransactionHandle;
import com.vastdata.trino.tx.VastTrinoTransactionHandleManager;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ArgumentSpecification;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.TypeManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.vastdata.client.error.VastExceptionFactory.closedTransaction;
import static com.vastdata.client.partition.PartitionConstants.IDENTITY_TRANSFORM;
import static com.vastdata.client.schema.VastMetadataUtils.PARTITIONED_BY_PROPERTY;
import static com.vastdata.client.schema.VastMetadataUtils.SORTED_BY_PROPERTY;
import static com.vastdata.trino.tablefunction.VastConnectorTableFunctionHandle.IDENTITY_PATTERN;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class VastConnector
        implements Connector
{
    private static final Logger LOG = Logger.get(VastConnector.class);
    private static final Pattern TRANSFORM_WITH_ARG_PATTERN = Pattern.compile(
            "^(\\w+)\\((.+),\\s*(.+)\\)$");
    private static final Pattern TRANSFORM_NO_ARG_PATTERN = Pattern.compile(
            "^(\\w+)\\((.+)\\)$");

    private final LifeCycleManager lifeCycleManager;
    private final TypeManager typeManager;
    private final VastClient client;
    private final VastConfig vastConfig;
    private final VastAccessControl accessControl;
    private final VastQueryEngineClient vastQueryEngineClient;
    private final VastTrinoTransactionHandleManager transManager;
    private final VastSplitManager splitManager;
    private final VastPageSourceProvider pageSourceProvider;
    private final VastPageSinkProvider pageSinkProvider;
    private final ConnectorNodePartitioningProvider nodePartitioningProvider;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final List<PropertyMetadata<?>> tableProperties;
    private final VastStatisticsManager statisticsManager;
    private final ObjectMapper objectMapper;
    private final RootAllocator allocator;
    private final ShapingLoggerFactory shapingLoggerFactory;

    @Inject
    public VastConnector(LifeCycleManager lifeCycleManager,
                         TypeManager typeManager,
                         VastClient client,
                         VastConfig vastConfig,
                         VastAccessControl accessControl,
                         VastQueryEngineClient vastQueryEngineClient,
                         VastTrinoTransactionHandleManager transManager,
                         VastSplitManager splitManager,
                         VastPageSourceProvider pageSourceProvider,
                         VastPageSinkProvider pageSinkProvider,
                         ConnectorNodePartitioningProvider nodePartitioningProvider,
                         VastStatisticsManager statisticsManager,
                         Set<SessionPropertiesProvider> sessionProperties,
                         ObjectMapper objectMapper,
                         RootAllocator allocator,
                         ShapingLoggerFactory shapingLoggerFactory)
    {
        LOG.info("Creating VAST connector: system=%s, hash=%s",
                VastVersion.SYS_VERSION, VastVersion.HASH);
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager,
                "lifeCycleManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.client = requireNonNull(client, "vast client is null");
        this.vastConfig = requireNonNull(vastConfig, "vast config is null");
        this.accessControl = requireNonNull(accessControl,
                "trinoConfig is null");
        this.vastQueryEngineClient = requireNonNull(vastQueryEngineClient,
                "vast query engine client is null");
        this.transManager = requireNonNull(transManager,
                "vast transaction factory is null");
        this.splitManager = requireNonNull(splitManager,
                "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider,
                "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider,
                "pageSinkProvider is null");
        this.nodePartitioningProvider = requireNonNull(nodePartitioningProvider,
                "nodePartitioningProvider is null");
        this.statisticsManager = requireNonNull(statisticsManager,
                "statisticsManager is null");
        this.objectMapper = requireNonNull(objectMapper,
                "objectMapperProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties,
                "sessionProperties is null")
                .stream()
                .flatMap(sessionPropertiesProvider -> sessionPropertiesProvider
                        .getSessionProperties()
                        .stream())
                .collect(toImmutableList());
        this.shapingLoggerFactory = shapingLoggerFactory;

        // Fail early if JVM is misconfigured (ORION-158296)
        try (ArrowBuf buf = this.allocator.buffer(1024)) {
            LOG.debug("Arrow buffer allocated: %s", buf);
        }

        tableProperties = ImmutableList.of(
                new PropertyMetadata<>(SORTED_BY_PROPERTY,
                        "Bucket sorting columns", new ArrayType(VARCHAR),
                        List.class, ImmutableList.of(), false,
                        value -> ((List<?>) value)
                                .stream()
                                .map(name -> ((String) name).toLowerCase(
                                        ENGLISH))
                                .collect(toImmutableList()), value -> value),
                new PropertyMetadata<>(PARTITIONED_BY_PROPERTY,
                        "partition by columns", new ArrayType(VARCHAR),
                        List.class, ImmutableList.of(), false,
                        value -> ((List<String>) value)
                                .stream()
                                .map(VastConnector::parsePartitionSpec)
                                .collect(toImmutableList()), value -> value));
    }

    private static Set<String> decodeColumnNames(Object object)
    {
        if (object == null) {
            return null;
        }

        Collection<?> columns = ((Collection<?>) object);
        return columns
                .stream()
                .peek(property -> requireNonNull(property,
                        String.format("columns %s can not contain null values",
                                columns)))
                .map(String.class::cast)
                .collect(toImmutableSet());
    }

    @VisibleForTesting
    public static PartitionColumnMetadata parsePartitionSpec(String spec)
    {
        String trimmed = spec.trim();
        Matcher m = TRANSFORM_WITH_ARG_PATTERN.matcher(trimmed);
        if (m.matches()) {
            String transform = m.group(1).toLowerCase(ENGLISH);
            String sourceCol = m.group(2).trim().toLowerCase(ENGLISH);
            Integer arg = Integer.parseInt(m.group(3).trim());
            String pitColName = sourceCol + "_" + transform;
            return new PartitionColumnMetadata(pitColName, null, sourceCol,
                    null, transform, arg);
        }
        m = TRANSFORM_NO_ARG_PATTERN.matcher(trimmed);
        if (m.matches()) {
            String transform = m.group(1).toLowerCase(ENGLISH);
            String sourceCol = m.group(2).trim().toLowerCase(ENGLISH);
            String pitColName = sourceCol + "_" + transform;
            return new PartitionColumnMetadata(pitColName, null, sourceCol,
                    null, transform, null);
        }
        String sourceCol = trimmed.toLowerCase(ENGLISH);
        return new PartitionColumnMetadata(sourceCol, null, sourceCol, null,
                IDENTITY_TRANSFORM, null);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel,
                                                       boolean readOnly,
                                                       boolean autoCommit)
    {
        LOG.debug("Starting transaction");
        return this.transManager.startTransaction(null);
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        LOG.debug("Committing transaction %s", transactionHandle);
        this.transManager.commit((VastTransactionHandle) transactionHandle,
                null);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        LOG.debug("Rolling back transaction %s", transactionHandle);
        this.transManager.rollback((VastTransactionHandle) transactionHandle,
                null);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session,
                                         ConnectorTransactionHandle transactionHandle)
    {
        LOG.info("Creating VAST metadata: system=%s, hash=%s, tx=%s",
                VastVersion.SYS_VERSION, VastVersion.HASH, transactionHandle);
        VastTransactionHandle vastTransHandle = (VastTransactionHandle) transactionHandle;
        if (!this.transManager.isOpen(vastTransHandle)) {
            throw closedTransaction(vastTransHandle);
        }
        return new VastMetadata(pageSourceProvider, client, vastConfig,
                vastTransHandle, this.statisticsManager, objectMapper,
                allocator, shapingLoggerFactory);
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
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return nodePartitioningProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getAnalyzeProperties()
    {
        return ImmutableList.of(
                new PropertyMetadata<>("columns", "Columns to be analyzed",
                        new ArrayType(VARCHAR), Set.class, null, false,
                        VastConnector::decodeColumnNames, value -> value));
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return List.of(
                stringProperty("column_stats", "Persistent Column Statistics",
                        null, false));
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

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return accessControl.isEnabled() ? accessControl : null;
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return ImmutableSet.of(new ConnectorTableFunction()
        {
            @Override
            public String getSchema()
            {
                return "vast";
            }

            @Override
            public String getName()
            {
                return "execute";
            }

            @Override
            public List<ArgumentSpecification> getArguments()
            {
                return List.of(ScalarArgumentSpecification
                        .builder()
                        .name("query")
                        .type(VARCHAR)
                        .build(), ScalarArgumentSpecification
                        .builder()
                        .name("enforce-identity")
                        .type(BOOLEAN)
                        .defaultValue(Boolean.TRUE)
                        .build());
            }

            @Override
            public ReturnTypeSpecification getReturnTypeSpecification()
            {
                Descriptor descriptor = Descriptor.descriptor(
                        ImmutableList.of("records"), ImmutableList.of(VARCHAR));
                return new ReturnTypeSpecification.DescribedTable(descriptor);
            }

            @Override
            public TableFunctionAnalysis analyze(ConnectorSession session,
                                                 ConnectorTransactionHandle transaction,
                                                 Map<String, Argument> arguments,
                                                 ConnectorAccessControl accessControl)
            {
                String query = ((Slice) ((ScalarArgument) arguments.get(
                        "query")).getValue()).toStringUtf8();
                boolean enforceIdentity = ((boolean) ((ScalarArgument) arguments.get(
                        "enforce-identity")).getValue());
                if (enforceIdentity) {
                    if (!IDENTITY_PATTERN.matcher(query).matches()) {
                        throw new RuntimeException(String.format(
                                "The query should match the pattern %s",
                                IDENTITY_PATTERN.pattern()));
                    }
                    else if (session.getIdentity().getGroups().isEmpty()) {
                        throw new RuntimeException(
                                "The query requires groups to be specified in the session identity");
                    }
                }
                return TableFunctionAnalysis
                        .builder()
                        .handle(new VastConnectorTableFunctionHandle(query,
                                enforceIdentity))
                        .build();
            }
        });
    }

    @Override
    public Optional<FunctionProvider> getFunctionProvider()
    {
        return Optional.of(
                new VastTableFunction(vastConfig, vastQueryEngineClient,
                        typeManager));
    }
}
