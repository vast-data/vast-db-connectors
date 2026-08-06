/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.vastdata.ShapingLoggerFactory;
import com.vastdata.TableLayout;
import com.vastdata.client.QueryDataExtraParams;
import com.vastdata.client.RequestsHeaders;
import com.vastdata.client.TableSpecifiers;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastObjectDetails;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastExceptionFactory;
import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.client.error.VastServerException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.partition.PartitionColumnMetadata;
import com.vastdata.client.schema.AlterColumnContext;
import com.vastdata.client.schema.AlterSchemaContext;
import com.vastdata.client.schema.AlterTableContext;
import com.vastdata.client.schema.CreateTableContext;
import com.vastdata.client.schema.DropTableContext;
import com.vastdata.client.schema.DropViewContext;
import com.vastdata.client.schema.TableColumnLifecycleContext;
import com.vastdata.client.schema.VastMetadataUtils;
import com.vastdata.client.schema.VastViewMetadata;
import com.vastdata.client.stats.VastStatistics;
import com.vastdata.trino.expression.VastExpression;
import com.vastdata.trino.expression.VastProjectionPushdown;
import com.vastdata.trino.partition.VastPartitionFunction;
import com.vastdata.trino.predicate.ComplexPredicate;
import com.vastdata.trino.predicate.VastConnectorExpressionPushdown;
import com.vastdata.trino.rowid.TrinoRowIDFieldFactory;
import com.vastdata.trino.statistics.VastStatisticsManager;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.vastdata.client.ParsedURL.PATH_SEPERATOR;
import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.BIG_CATALOG_TABLE_NAME;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.IMPORT_DATA_HIDDEN_COLUMN_NAME;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.IMPORT_DATA_HIDDEN_FIELD;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.getBigCatalogSearchPath;
import static com.vastdata.client.partition.PartitionConstants.IDENTITY_TRANSFORM;
import static com.vastdata.client.partition.PartitionConstants.PIT_COLUMN_NAMES_TO_HIDE;
import static com.vastdata.client.partition.PartitionConstants.PIT_NAME_SUFFIX;
import static com.vastdata.client.rowid.RowIDStrategyType.UNSIGNED_INT64;
import static com.vastdata.client.schema.ArrowSchemaUtils.VASTDB_EXTERNAL_ROW_ID_COLUMN_NAME;
import static com.vastdata.client.schema.VastMetadataUtils.PARTITIONED_BY_PROPERTY;
import static com.vastdata.client.schema.VastMetadataUtils.SORTED_BY_PROPERTY;
import static com.vastdata.trino.VastSessionProperties.getClientPageSize;
import static com.vastdata.trino.VastSessionProperties.getComplexPredicatePushdown;
import static com.vastdata.trino.VastSessionProperties.getExpressionProjectionPushdown;
import static com.vastdata.trino.VastSessionProperties.getMatchSubstringPushdown;
import static com.vastdata.trino.ViewDefinitionHelpers.getVastViewMetadata;
import static com.vastdata.trino.ViewDefinitionHelpers.pageToViewDefinition;
import static com.vastdata.trino.ViewDefinitionHelpers.viewPageSource;
import static com.vastdata.trino.expression.VastProjectionPushdown.forVariableChildren;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class VastMetadata
        implements ConnectorMetadata
{
    public static final VastColumnHandle IMPORT_DATA_HIDDEN_COLUMN_HANDLE = new VastColumnHandle(
            IMPORT_DATA_HIDDEN_FIELD);

    public static final String PARTITIONS_TABLE_SUFFIX = "$partitions";
    private static final Logger LOG = Logger.get(VastMetadata.class);
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";
    private static final String SYSTEM_SCHEMA_NAME = "system_schema";

    private final VastClient client;
    private final VastConfig vastConfig;
    private final VastPageSourceProvider pageSourceProvider;
    private final VastTransactionHandle transactionHandle;
    private final VastMetadataUtils util = new VastMetadataUtils();
    private final VastTrinoExceptionFactory vastTrinoExceptionFactory = new VastTrinoExceptionFactory();
    private final VastStatisticsManager statisticsManager;
    private final ObjectMapper objectMapper;
    private final BufferAllocator allocator;
    private final ShapingLoggerFactory shapingLoggerFactory;

    public VastMetadata(VastPageSourceProvider pageSourceProvider,
                        VastClient client,
                        VastConfig vastConfig,
                        VastTransactionHandle transactionHandle,
                        VastStatisticsManager statisticsManager,
                        ObjectMapper objectMapper,
                        RootAllocator rootAllocator,
                        ShapingLoggerFactory shapingLoggerFactory)
    {
        this.pageSourceProvider = pageSourceProvider;
        this.client = requireNonNull(client);
        this.vastConfig = vastConfig;
        this.transactionHandle = requireNonNull(transactionHandle);
        this.statisticsManager = requireNonNull(statisticsManager);
        this.objectMapper = requireNonNull(objectMapper);
        this.allocator = rootAllocator.newChildAllocator("VastMetadata", 0,
                Long.MAX_VALUE);
        this.shapingLoggerFactory = shapingLoggerFactory;
    }

    private static String toVastSchemaName(ConnectorSession session,
                                           String schemaName)
    {
        if (VastSessionProperties.getEnableCustomSchemaSeparator(session)) {
            return schemaName.replace(
                    VastSessionProperties.getCustomSchemaSeparator(session),
                    PATH_SEPERATOR);
        }
        else {
            return schemaName;
        }
    }

    private static String fromVastSchemaName(ConnectorSession session,
                                             String schemaName)
    {
        if (VastSessionProperties.getEnableCustomSchemaSeparator(session)) {
            return schemaName.replace(PATH_SEPERATOR,
                    VastSessionProperties.getCustomSchemaSeparator(session));
        }
        else {
            return schemaName;
        }
    }

    private static SchemaTableName toVastSchemaTableName(ConnectorSession session,
                                                         SchemaTableName schemaTableName)
    {
        return new SchemaTableName(
                toVastSchemaName(session, schemaTableName.getSchemaName()),
                schemaTableName.getTableName());
    }

    static Optional<VastSubstringMatch> tryParseSubstringMatch(
            ConnectorExpression conjunct,
            Set<String> pushedDownColumnNames,
            Map<String, ColumnHandle> assignments)
    {
        if (!(conjunct instanceof Call call)) {
            return Optional.empty();
        }
        if (!call.getFunctionName().equals(LIKE_FUNCTION_NAME)) {
            return Optional.empty();
        }
        List<ConnectorExpression> args = call.getArguments();
        if (args.size() != 2) {
            return Optional.empty();  // no support for escaped LIKE expression
        }
        if (!(args.getFirst() instanceof Variable variable)) {
            return Optional.empty(); // projection pushdown should handle `substring_match` over nested columns
        }
        if (pushedDownColumnNames.contains(variable.getName())) {
            return Optional.empty(); // no support for AND between TupleDomain and ConnectorExpression on the same column
        }
        Constant constant = (Constant) args.get(1);
        Slice slice = (Slice) constant.getValue();
        String pattern = slice.toStringUtf8();
        if (!(pattern.startsWith("%") && pattern.endsWith(
                "%") && pattern.length() > 2)) {
            return Optional.empty(); // handle only "substring" LIKE expressions
        }
        // remove leading and trailing wildcards
        String substring = pattern.substring(1, pattern.length() - 1);
        if (substring.contains("%") || substring.contains(
                "_") || substring.contains("\\")) {
            return Optional.empty(); // no support for inner wildcards or escaping (for simplicity)
        }
        VastColumnHandle column = (VastColumnHandle) requireNonNull(
                assignments.get(variable.getName()),
                () -> format("missing %s in %s", variable, assignments));
        return Optional.of(new VastSubstringMatch(column, substring));
    }

    @Override
    public boolean supportsMissingColumnsOnInsert()
    {
        return true;
    }

    private ConnectorTableMetadata toVastConnectorTableMetadata(ConnectorSession session,
                                                                ConnectorTableMetadata tableMetadata)
    {
        return new ConnectorTableMetadata(
                toVastSchemaTableName(session, tableMetadata.getTable()),
                tableMetadata.getColumns(), tableMetadata.getProperties(),
                tableMetadata.getComment());
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        final String endUser = session.getUser();
        int clientPageSize = VastSessionProperties.getClientPageSize(session);
        LOG.debug("tx %s: listSchemaNames(%s) endUser=%s", transactionHandle,
                clientPageSize, endUser);
        try {
            return client.listAllSchemas(transactionHandle, clientPageSize,
                            endUser)
                    // TODO https://github.com/trinodb/trino/issues/1559 this should be filtered out in engine.
                    .filter(schemaName ->
                    {
                        if (INFORMATION_SCHEMA_NAME.equalsIgnoreCase(
                                schemaName)) {
                            LOG.warn(new RuntimeException(
                                            format("Got internal schema name: \"%s\" - skipping",
                                                    schemaName)),
                                    "schemaName equalsIgnoreCase %s - skipping",
                                    SYSTEM_SCHEMA_NAME);
                            return false;
                        }
                        return true;
                    })
                    .map(schemaName -> fromVastSchemaName(session, schemaName))
                    .collect(toList());
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        catch (VastRuntimeException re) {
            throw vastTrinoExceptionFactory.fromVastRuntimeException(re);
        }
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        final String endUser = session.getUser();
        schemaName = toVastSchemaName(session, schemaName);
        LOG.debug("tx %s: schemaExists(%s) endUser=%s", transactionHandle,
                schemaName, endUser);
        try {
            return client.schemaExists(transactionHandle, schemaName, endUser);
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        catch (VastRuntimeException re) {
            throw vastTrinoExceptionFactory.fromVastRuntimeException(re);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session,
                                               SchemaTableName schemaTableName,
                                               Optional<ConnectorTableVersion> startVersion,
                                               Optional<ConnectorTableVersion> endVersion)
    {
        final String endUser = session.getUser();
        schemaTableName = toVastSchemaTableName(session, schemaTableName);
        LOG.debug("getTableHandle schemaTableName: %s, tx: %s, endUser: %s",
                schemaTableName, transactionHandle, endUser);
        Optional<String> bigCatalogSearchPath = getBigCatalogSearchPath(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName());
        if (bigCatalogSearchPath.isPresent()) {
            LOG.debug("tx %s: getTableHandle name=%s start=%s end=%s",
                    transactionHandle, schemaTableName, startVersion,
                    endVersion);
            VastTableHandle tableHandle = (VastTableHandle) getTableHandle(
                    session,
                    new SchemaTableName(schemaTableName.getSchemaName(),
                            BIG_CATALOG_TABLE_NAME), startVersion, endVersion);
            if (tableHandle != null) {
                tableHandle.addExtraQueryParams(QueryDataExtraParams.QueryDataExtraParamType.HEADER, RequestsHeaders.TABULAR_MST_POINTER.getHeaderName(), Long.toString(tableHandle.getHandleID().getMstPointer()));
                return tableHandle.withBigCatalogSearchPath(
                        bigCatalogSearchPath.orElseThrow());
            }
            else {
                throw new IllegalStateException(
                        format("Table handle for Big Catalog was not found: %s",
                                schemaTableName));
            }
        }
        TableSpecifiers tableSpecifiers = TableSpecifiers.parse(
                schemaTableName.getTableName());
        String tableNameForExistenceCheck = tableSpecifiers.getTableName();
        try {
            if (tableNameForExistenceCheck.contains(PARTITIONS_TABLE_SUFFIX)) {
                tableNameForExistenceCheck = tableNameForExistenceCheck
                        .substring(0, tableNameForExistenceCheck.indexOf(
                                PARTITIONS_TABLE_SUFFIX))
                        .trim();
            }
            Optional<VastObjectDetails> vastTableHandleId = client.getVastTableHandleId(
                    transactionHandle, schemaTableName.getSchemaName(),
                    tableNameForExistenceCheck, endUser);
            if (vastTableHandleId.isPresent()) {
                VastTableHandle vastTableHandle;
                if (tableSpecifiers
                        .getTableName()
                        .contains(PARTITIONS_TABLE_SUFFIX)) {
                    String partitionTableName = tableNameForExistenceCheck + PIT_NAME_SUFFIX;
                    vastTableHandle = new VastTableHandle(
                            schemaTableName.getSchemaName(), partitionTableName,
                            vastTableHandleId.orElseThrow(), false,
                            tableSpecifiers.isForNonAcidOperation());
                }
                else {
                    vastTableHandle = new VastTableHandle(
                            schemaTableName.getSchemaName(),
                            tableNameForExistenceCheck,
                            vastTableHandleId.orElseThrow(),
                            tableSpecifiers.isForImportDataOperation(),
                            tableSpecifiers.isForNonAcidOperation());
                }
                vastTableHandle.getExtraQueryParams().addExtraQueryParams(QueryDataExtraParams.QueryDataExtraParamType.HEADER, RequestsHeaders.TABULAR_MST_POINTER.getHeaderName(), Long.toString(vastTableHandleId.orElseThrow().getMstPointer()));

                return populateMetadata(vastTableHandle,
                        getClientPageSize(session), endUser);
            }
            else {
                return null;
            }
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        catch (VastRuntimeException re) {
            throw vastTrinoExceptionFactory.fromVastRuntimeException(re);
        }
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session,
                                               ConnectorTableHandle tableHandle)
    {
        final String endUser = session.getUser();
        int clientPageSize = getClientPageSize(session);
        LOG.debug("tx %s: getTableSchema(%s) endUser=%s", transactionHandle,
                tableHandle, endUser);
        VastTableHandle table = (VastTableHandle) tableHandle;
        try {
            table = populateMetadata(table, clientPageSize, endUser);
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        SchemaTableName schemaTableName = table.toSchemaTableName();
        List<ColumnSchema> columns = table
                .getColumnHandlesCache()
                .stream()
                .map(VastColumnHandle::getColumnSchema)
                .collect(toList());
        return new ConnectorTableSchema(schemaTableName, columns);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
                                                   ConnectorTableHandle tableHandle)
    {
        int clientPageSize = VastSessionProperties.getClientPageSize(session);
        LOG.debug("tx %s: getTableMetadata(%s, %s)", transactionHandle,
                tableHandle, clientPageSize);
        VastTableHandle table = (VastTableHandle) tableHandle;
        try {
            table = populateMetadata(table, clientPageSize, session.getUser());
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        List<String> sColumns = table.getSortedColumns().orElse(List.of());
        List<PartitionColumnMetadata> pColumns = table
                .getPartitionColumns()
                .orElse(List.of());
        List<ColumnMetadata> columns = table
                .getColumnHandlesCache()
                .stream()
                .map(VastColumnHandle::getColumnMetadata)
                .collect(toList());
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        if (!sColumns.isEmpty()) {
            properties.put(SORTED_BY_PROPERTY, sColumns);
        }
        if (!pColumns.isEmpty()) {
            List<String> pColumnStrings = pColumns.stream().map(pcm ->
            {
                if (pcm.arg != null) {
                    return String.format("%s(%s, %d)", pcm.transform,
                            pcm.sourceColumnName, pcm.arg);
                }
                else if (!IDENTITY_TRANSFORM.equalsIgnoreCase(pcm.transform)) {
                    return String.format("%s(%s)", pcm.transform,
                            pcm.sourceColumnName);
                }
                else {
                    return pcm.sourceColumnName;
                }
            }).collect(toList());
            properties.put(PARTITIONED_BY_PROPERTY, pColumnStrings);
        }
        ConnectorTableMetadata result = new ConnectorTableMetadata(
                table.toSchemaTableName(), columns, properties.build());
        LOG.debug("%s", result);
        return result;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session,
                                            Optional<String> optionalSchemaName)
    {
        final String endUser = session.getUser();
        int clientPageSize = VastSessionProperties.getClientPageSize(session);
        LOG.debug("tx %s: listTables(%s, %s) endUser=%s", transactionHandle,
                optionalSchemaName, clientPageSize, endUser);
        final List<SchemaTableName> tables = optionalSchemaName
                .map(Stream::of)
                .orElseGet(() -> listSchemaNames(session).stream())
                .flatMap(schemaName ->
                {
                    String vastSchemaName = toVastSchemaName(session,
                            schemaName);
                    if (vastSchemaName.equalsIgnoreCase(
                            INFORMATION_SCHEMA_NAME)) {
                        // TODO https://github.com/trinodb/trino/issues/1559 this should be filtered out in engine.
                        LOG.warn(new RuntimeException(
                                        format("Got internal schema name: \"%s\" - skipping",
                                                schemaName)),
                                "schemaName equalsIgnoreCase %s - skipping",
                                INFORMATION_SCHEMA_NAME);
                        return Stream.empty();
                    }
                    if (vastSchemaName.equalsIgnoreCase(SYSTEM_SCHEMA_NAME)) {
                        // TODO: workaround for ORION-154485
                        LOG.warn(new RuntimeException(
                                        format("Got internal schema name: \"%s\" - skipping",
                                                schemaName)),
                                "schemaName equalsIgnoreCase %s - skipping",
                                SYSTEM_SCHEMA_NAME);
                        return Stream.empty();
                    }
                    try {
                        return client
                                .listTables(transactionHandle, vastSchemaName,
                                        clientPageSize, endUser)
                                .map(table -> new SchemaTableName(
                                        schemaName, table.getName()));
                    }
                    catch (VastServerException e) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
                    }
                    catch (VastUserException e) {
                        throw new TrinoException(GENERIC_USER_ERROR, e);
                    }
                })
                .toList();
        final List<SchemaTableName> views = listViews(session,
                optionalSchemaName);
        final List<SchemaTableName> tablesAndViews = new ArrayList<>(
                tables.size() + views.size());
        tablesAndViews.addAll(tables);
        tablesAndViews.addAll(views);
        return tablesAndViews;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
                                                      ConnectorTableHandle tableHandle)
    {
        final String endUser = session.getUser();
        int clientPageSize = VastSessionProperties.getClientPageSize(session);
        LOG.debug("tx %s: getColumnHandles(%s, %s) endUser=%s",
                transactionHandle, tableHandle, clientPageSize, endUser);
        VastTableHandle table = (VastTableHandle) tableHandle;
        Map<String, ColumnHandle> result = table
                .getColumnHandlesCache()
                .stream()
                .collect(Collectors.toMap(col -> col.getField().getName(),
                        Function.identity()));
        LOG.debug("%s", result);
        return result;
    }

    private VastTableHandle populateMetadata(VastTableHandle table,
                                             int pageSize,
                                             final String endUser)
            throws VastException
    {
        if (table.getColumnHandlesCache() != null) {
            return table;
        }

        TableLayout layout;

        if (table.getSchemaName().equalsIgnoreCase(INFORMATION_SCHEMA_NAME)) {
            // TODO https://github.com/trinodb/trino/issues/1559 this should be filtered out in engine.
            layout = TableLayout.EMPTY;
        }
        else {
            layout = client.fetchTableLayout(transactionHandle,
                    table.getSchemaName(), table.getTableName(), pageSize,
                    table.getExtraQueryParams(), endUser);
        }

        if (table.isPit()) {
            layout = new TableLayout(new Schema(layout
                    .getSchema()
                    .getFields()
                    .stream()
                    .filter(field -> !PIT_COLUMN_NAMES_TO_HIDE.contains(
                            field.getName()))
                    .collect(toList())), List.of(), List.of());
        }

        List<VastColumnHandle> columns = layout
                .getSchema()
                .getFields()
                .stream()
                .map(VastColumnHandle::fromField)
                .toList();

        if (table.getForImportData()) {
            List<VastColumnHandle> columnHandlesWithImportDataColumn = new ArrayList<>();
            columnHandlesWithImportDataColumn.add(
                    IMPORT_DATA_HIDDEN_COLUMN_HANDLE);
            columnHandlesWithImportDataColumn.addAll(columns);
            columns = columnHandlesWithImportDataColumn;
        }

        table.setColumnHandlesCache(columns);

        if (layout.hasSortedColumns()) {
            table = table.withSortedColumns(layout
                    .getSortedColumns()
                    .stream()
                    .map(Field::getName)
                    .collect(toList()));
        }

        if (layout.hasPartitionColumns()) {
            table = table.withPartitionColumns(
                    layout.getPartitionColumnsMetadata());
        }

        return table;
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        final String endUser = session.getUser();
        int clientPageSize = VastSessionProperties.getClientPageSize(session);
        LOG.debug(
                "streamTableColumns: prefix=%s, clientPageSize=%s, tx=%s, endUser=%s",
                schemaName, clientPageSize, transactionHandle, endUser);
        if (schemaName.isPresent()) {
            Set<SchemaTableName> all = new HashSet<>();
            relationFilter.apply(all);
            return all.stream().map(schemaTableName ->
            {
                VastTableHandle vastTableHandle = (VastTableHandle) getTableHandle(
                        session, schemaTableName, Optional.empty(),
                        Optional.empty());
                List<ColumnMetadata> columnMetadata = vastTableHandle
                        .getColumnHandlesCache()
                        .stream()
                        .map(vch -> getColumnMetadata(session, vastTableHandle,
                                vch))
                        .toList();
                return new RelationColumnsMetadata(schemaTableName,
                        Optional.empty(), Optional.empty(),
                        Optional.of(columnMetadata), false);
            }).iterator();
        }
        return Collections.emptyIterator();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
                                            ConnectorTableHandle tableHandle,
                                            ColumnHandle columnHandle)
    {
        LOG.debug("getColumnMetadata: tableHandle=%s, columnHandle=%s, tx=%s",
                tableHandle, columnHandle, transactionHandle);
        return ((VastColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session,
                                                       ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session,
                                                            ConnectorTableMetadata tableMetadata)
    {
        List<PartitionColumnMetadata> partitionColumns = (List<PartitionColumnMetadata>) tableMetadata
                .getProperties()
                .get(PARTITIONED_BY_PROPERTY);

        if (partitionColumns.isEmpty()) {
            return Optional.empty();
        }

        List<Field> fields = tableMetadata.getColumns().stream().map(column ->
        {
            return TypeUtils.convertTrinoTypeToArrowField(column.getType(),
                    column.getName(), column.isNullable());
        }).toList();

        List<VastColumnHandle> columnHandles = fields
                .stream()
                .map(VastColumnHandle::fromField)
                .toList();
        VastPartitioningHandle partitionHandle = VastPartitioningHandle.create(
                partitionColumns, columnHandles);

        List<String> columnNames = partitionHandle
                .partitionFunctions()
                .stream()
                .map(VastPartitionFunction::columnName)
                .toList();

        return Optional.of(
                new ConnectorTableLayout(partitionHandle, columnNames, false));
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session,
                                                          ConnectorTableHandle tableHandle)
    {
        VastTableHandle vastTableHandle = (VastTableHandle) tableHandle;
        if (vastTableHandle.getForImportData()) {
            return Optional.empty();
        }
        return vastTableHandle.getPartitioningHandle().map(partitioningHandle ->
        {
            List<String> partitionColumnNames = partitioningHandle
                    .partitionFunctions()
                    .stream()
                    .map(VastPartitionFunction::columnName)
                    .collect(toImmutableList());
            return new ConnectorTableLayout(partitioningHandle,
                    partitionColumnNames, true);
        });
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(
            ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        Optional<ConnectorPartitioningHandle> ret = Optional.empty();
        VastTableHandle vastTableHandle = (VastTableHandle) tableHandle;
        if (vastTableHandle.getPartitioningHandle().isPresent()) {
            Type columnType = vastTableHandle.getRowIdStrategyType() == UNSIGNED_INT64 ?
                    IntegerType.INTEGER :
                    DecimalType.createDecimalType(38, 0);
            PartitionColumnMetadata pcm = new PartitionColumnMetadata(
                    VASTDB_EXTERNAL_ROW_ID_COLUMN_NAME,
                    columnType.getDisplayName(),
                    VASTDB_EXTERNAL_ROW_ID_COLUMN_NAME,
                    columnType.getDisplayName(),
                    IDENTITY_TRANSFORM.toUpperCase(Locale.ROOT), null);
            VastPartitionFunction vastPartitionFunction = VastPartitionFunction.create(
                    pcm, columnType, 0, 0);
            ret = Optional.of(
                    new VastPartitioningHandle(List.of(vastPartitionFunction)));
        }
        return ret;
    }

    @Override
    public Optional<ConnectorTableHandle> applyPartitioning(ConnectorSession session,
                                                            ConnectorTableHandle tableHandle,
                                                            Optional<ConnectorPartitioningHandle> partitioningHandle,
                                                            List<ColumnHandle> columns)
    {
        VastTableHandle vastTableHandle = (VastTableHandle) tableHandle;
        if (vastTableHandle.getPartitionColumns().isPresent()) {
            return Optional.of(tableHandle);
        }
        return Optional.empty();
    }

    @Override
    public void addColumn(final ConnectorSession session,
                          final ConnectorTableHandle tableHandle,
                          final ColumnMetadata column,
                          final ColumnPosition position)
    {
        final String endUser = session.getUser();
        LOG.debug("addColumn: column=%s table=%s, tx=%s, endUser=%s", column,
                tableHandle, transactionHandle, endUser);
        String name = column.getName();
        if (name.equals(IMPORT_DATA_HIDDEN_COLUMN_NAME)) {
            throw new TrinoException(GENERIC_USER_ERROR,
                    format("Illegal name for add column: %s", name));
        }
        VastTableHandle table = (VastTableHandle) tableHandle;
        TableColumnLifecycleContext ctx = new VastTrinoSchemaAdaptor().adaptForAddColumn(
                tableHandle, column);
        try {
            client.addColumn(transactionHandle, ctx, endUser);
            table.clearColumnHandlesCache();
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session,
                           ConnectorTableHandle tableHandle,
                           ColumnHandle column)
    {
        final String endUser = session.getUser();
        LOG.debug("dropColumn: column=%s, table=%s, tx=%s, endUser=%s", column,
                tableHandle, transactionHandle, endUser);
        TableColumnLifecycleContext ctx = new VastTrinoSchemaAdaptor().adaptForDropColumn(
                tableHandle, column);
        try {
            client.dropColumn(transactionHandle, ctx, endUser);
            VastTableHandle table = (VastTableHandle) tableHandle;
            table.clearColumnHandlesCache();
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
    }

    @Override
    public void dropTable(ConnectorSession session,
                          ConnectorTableHandle tableHandle)
    {
        final String endUser = session.getUser();
        LOG.debug("dropTable: table=%s, tx=%s, endUser=%s", tableHandle,
                transactionHandle, endUser);
        DropTableContext ctx = new VastTrinoSchemaAdaptor().adaptForDropTable(
                tableHandle);
        try {
            client.dropTable(transactionHandle, ctx, endUser);
            VastTableHandle table = (VastTableHandle) tableHandle;
            table.clearColumnHandlesCache();
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
    }

    @Override
    public void dropSchema(ConnectorSession session,
                           String schemaName,
                           boolean cascade)
    {
        final String endUser = session.getUser();
        // TODO Add support for DROP SCHEMA CASCADE/RESTRICT
        // https://vastdata.atlassian.net/browse/ORION-151959
        schemaName = toVastSchemaName(session, schemaName);
        LOG.debug("dropSchema: schema=%s, tx=%s, endUser=%s", schemaName,
                transactionHandle, endUser);
        try {
            client.dropSchema(transactionHandle, schemaName, endUser);
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        catch (VastRuntimeException e) {
            throw vastTrinoExceptionFactory.fromVastRuntimeException(e);
        }
    }

    @Override
    public void createSchema(ConnectorSession session,
                             String schemaName,
                             Map<String, Object> properties,
                             TrinoPrincipal owner)
    {
        schemaName = toVastSchemaName(session, schemaName);
        String serializedProperties = util.getPropertiesString(properties);
        LOG.info("createSchema: schema=%s, properties=%s, owner=%s, tx=%s",
                schemaName, serializedProperties, owner, transactionHandle);
        try {
            client.createSchema(transactionHandle, schemaName,
                    serializedProperties, null);
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        catch (VastRuntimeException e) {
            throw vastTrinoExceptionFactory.fromVastRuntimeException(e);
        }
    }

    @Override
    public void createTable(final ConnectorSession session,
                            final ConnectorTableMetadata tableMetadata,
                            final SaveMode saveMode)
    {
        if (saveMode == SaveMode.REPLACE) {
            throw new TrinoException(NOT_SUPPORTED,
                    "VAST connector does not support replacing tables");
        }
        final String endUser = session.getUser();
        final ConnectorTableMetadata vastTableMetadata = toVastConnectorTableMetadata(
                session, tableMetadata);
        LOG.debug(
                "createTable: vastTableMetadata=%s, saveMode=%s, tx=%s, endUser=%s",
                vastTableMetadata, saveMode, transactionHandle, endUser);
        TableSpecifiers tableSpecifiers = TableSpecifiers.parse(
                vastTableMetadata.getTable().getTableName());
        if (tableSpecifiers.hasOperationSpecifiers()) {
            throw new TrinoException(GENERIC_USER_ERROR,
                    format("Illegal table name for create table: %s",
                            vastTableMetadata.getTable().getTableName()));
        }
        if (saveMode == SaveMode.IGNORE) {
            ConnectorTableHandle table = getTableHandle(session,
                    vastTableMetadata.getTable(), Optional.empty(),
                    Optional.empty());
            if (nonNull(table)) {
                LOG.info("createTable: table %s already exists, tx=%s", table,
                        transactionHandle);
                return;
            }
        }
        try {
            CreateTableContext ctx = new VastTrinoSchemaAdaptor().adaptForCreateTable(
                    vastTableMetadata, objectMapper);
            client.createTable(transactionHandle, ctx, endUser);
        }
        catch (VastUserException e) {
            throw vastTrinoExceptionFactory.wrapVastErrorWithInvalidPropertyError(
                    e);
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        catch (VastRuntimeException e) {
            throw vastTrinoExceptionFactory.fromVastRuntimeException(e);
        }
    }

    private void validateColumnsList(List<String> propertyValues,
                                     List<String> columnNames,
                                     String propertyName)
    {
        if (!new HashSet<>(columnNames
                .stream()
                .map(String::toLowerCase)
                .collect(toList())).containsAll(propertyValues)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY,
                    format("Invalid column names in '%s'. expected cols '%s' to contain propertyValues: '%s'",
                            propertyName, columnNames, propertyValues));
        }
        Set<String> propertySet = new HashSet<>(propertyValues);
        if (propertyValues.size() != propertySet.size()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY,
                    format("Each column can only appear once in %s",
                            propertyName));
        }
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(final ConnectorSession session,
                                                       final ConnectorTableMetadata tableMetadata,
                                                       final Optional<ConnectorTableLayout> layout,
                                                       final RetryMode retryMode,
                                                       final boolean replace)
    {
        final String endUser = session.getUser();
        final ConnectorTableMetadata vastTableMetadata = toVastConnectorTableMetadata(
                session, tableMetadata);
        final int clientPageSize = VastSessionProperties.getClientPageSize(
                session);
        LOG.debug(
                "beginCreateTable: vastTableMetadata=%s, layout=%s, retryMode=%s, clientPageSize=%s, tx=%s, endUser=%s",
                vastTableMetadata, layout, retryMode, clientPageSize,
                transactionHandle, endUser);
        createTable(session, vastTableMetadata, SaveMode.FAIL);
        VastTableHandle table = (VastTableHandle) getTableHandle(session,
                vastTableMetadata.getTable(), Optional.empty(),
                Optional.empty());
        if (table != null) {
            List<VastColumnHandle> columns = new ArrayList<>(
                    table.getColumnHandlesCache());
            return new VastInsertTableHandle(table, columns, true, false, layout
                    .flatMap(ConnectorTableLayout::getPartitioning)
                    .map(VastPartitioningHandle.class::cast)); // used for `CREATE TABLE t AS SELECT ...`
        }
        else {
            throw new TrinoException(TABLE_NOT_FOUND,
                    format("Table doesn't exist: %s", vastTableMetadata));
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session,
                                                               ConnectorOutputTableHandle tableHandle,
                                                               Collection<Slice> fragments,
                                                               Collection<ComputedStatistics> computedStatistics)
    {
        LOG.debug(
                "finishCreateTable: tableHandle=%s, fragments=%s, computedStatistics=%s, tx=%s",
                tableHandle, fragments, computedStatistics, transactionHandle);
        return Optional.empty();
    }

    @Override
    public List<SchemaTableName> listViews(final ConnectorSession session,
                                           final Optional<String> viewSchemaName)
    {
        final String endUser = session.getUser();
        final int clientPageSize = VastSessionProperties.getClientPageSize(
                session);
        LOG.debug(
                "listTables: viewSchemaName=%s, clientPageSize=%s, tx=%s, endUser=%s",
                viewSchemaName, clientPageSize, transactionHandle, endUser);
        return viewSchemaName
                .map(Stream::of)
                .orElseGet(() -> listSchemaNames(session).stream())
                .flatMap(schemaName ->
                {
                    final String vastSchemaName = toVastSchemaName(session,
                            schemaName);
                    if (vastSchemaName.equalsIgnoreCase(
                            INFORMATION_SCHEMA_NAME)) {
                        // TODO https://github.com/trinodb/trino/issues/1559 this should be filtered out in engine.
                        LOG.warn(new RuntimeException(
                                        format("Got internal schema name: \"%s\" - skipping",
                                                schemaName)),
                                "schemaName equalsIgnoreCase %s - skipping",
                                INFORMATION_SCHEMA_NAME);
                        return Stream.empty();
                    }
                    if (vastSchemaName.equalsIgnoreCase(SYSTEM_SCHEMA_NAME)) {
                        // TODO: workaround for ORION-154485
                        LOG.warn(new RuntimeException(
                                        format("Got internal schema name: \"%s\" - skipping",
                                                schemaName)),
                                "schemaName equalsIgnoreCase %s - skipping",
                                SYSTEM_SCHEMA_NAME);
                        return Stream.empty();
                    }
                    try {
                        return client
                                .listViews(transactionHandle, vastSchemaName,
                                        clientPageSize, endUser)
                                .map(viewName -> new SchemaTableName(schemaName,
                                        viewName));
                    }
                    catch (final VastServerException error) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, error);
                    }
                    catch (VastUserException error) {
                        throw new TrinoException(GENERIC_USER_ERROR, error);
                    }
                })
                .toList();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session,
                                                     SchemaTableName viewName)
    {
        final String endUser = session.getUser();
        final SchemaTableName vastViewName = toVastSchemaTableName(session,
                viewName);
        try {
            if (client.schemaExists(transactionHandle,
                    vastViewName.getSchemaName(), endUser)) {
                if (client.viewExists(transactionHandle,
                        vastViewName.getSchemaName(),
                        vastViewName.getTableName(), endUser)) {
                    try (VastPageSource source = viewPageSource(
                            pageSourceProvider, vastViewName, transactionHandle,
                            client, session)) {
                        SourcePage page = null;
                        while (!source.isFinished()) {
                            page = source.getNextSourcePage();
                            if (page.getChannelCount() > 0) {
                                break;
                            }
                        }
                        LOG.debug("getView: page=%s", page);
                        if (page == null || page.getChannelCount() == 0) {
                            throw vastTrinoExceptionFactory.fromThrowable(
                                    VastExceptionFactory.serverException(
                                            format("Failed getting view metadata for view: %s",
                                                    vastViewName)));
                        }
                        final List<Field> underlyingColumns = client
                                .listColumns(transactionHandle,
                                        vastViewName.getSchemaName(),
                                        vastViewName.getTableName(), 1000,
                                        new QueryDataExtraParams(), endUser)
                                .getFields();
                        LOG.debug("getView: underlyingColumns=%s",
                                underlyingColumns);
                        return Optional.of(
                                pageToViewDefinition(page, underlyingColumns,
                                        vastViewName.getSchemaName()));
                    }
                }
                else {
                    LOG.debug("getView: view is not in schema=%s, tx=%s",
                            vastViewName, transactionHandle);
                    return Optional.empty();
                }
            }
            else {
                LOG.debug("getView: no such schema: %s, tx=%s", vastViewName,
                        transactionHandle);
                return Optional.empty();
            }
        }
        catch (final VastException error) {
            LOG.error(error, "getView failed: vastViewName=%s, tx=%s",
                    vastViewName, transactionHandle);
            throw new RuntimeException(error);
        }
    }

    @Override
    public void createView(final ConnectorSession session,
                           final SchemaTableName viewName,
                           final ConnectorViewDefinition definition,
                           final Map<String, Object> viewProperties,
                           final boolean replace)
    {
        final String endUser = session.getUser();
        final SchemaTableName vastViewName = toVastSchemaTableName(session,
                viewName);
        LOG.debug(
                "createView: definition=%s, vastViewName=%s, tx=%s, endUser=%s",
                definition, vastViewName, transactionHandle, endUser);
        if (!definition.getPath().isEmpty()) {
            throw new IllegalStateException(
                    format("path expected to be empty but path = %s",
                            definition.getPath()));
        }
        final VastViewMetadata context = getVastViewMetadata(definition,
                viewProperties, vastViewName);
        try {
            if (client.schemaExists(transactionHandle,
                    vastViewName.getSchemaName(), endUser)) {
                if (!client.viewExists(transactionHandle,
                        vastViewName.getSchemaName(),
                        vastViewName.getTableName(), endUser)) {
                    client.createView(transactionHandle, context, endUser);
                    LOG.debug(
                            "createView: created vastViewName=%s, tx=%s, endUser=%s",
                            vastViewName, transactionHandle, endUser);
                }
                else {
                    if (replace) {
                        client.dropView(transactionHandle, new DropViewContext(
                                vastViewName.getSchemaName(),
                                vastViewName.getTableName()), endUser);
                        LOG.debug(
                                "createView: dropped existing vastViewName=%s, tx=%s, endUser=%s",
                                vastViewName, transactionHandle, endUser);
                        client.createView(transactionHandle, context, endUser);
                        LOG.debug(
                                "createView: re-created vastViewName=%s, tx=%s, endUser=%s",
                                vastViewName, transactionHandle, endUser);
                    }
                    else {
                        final RuntimeException error = new ViewAlreadyExistsException(
                                viewName);
                        LOG.error(error,
                                format("createView: failed creating view. vastViewName=%s - already exists, tx=%s, endUser=%s",
                                        vastViewName.getSchemaName(),
                                        transactionHandle, endUser));
                        throw error;
                    }
                }
            }
            else {
                final RuntimeException error = new SchemaNotFoundException(
                        viewName.getSchemaName());
                LOG.error(error,
                        format("createView: failed creating view. vastViewName=%s - schema not found, tx=%s",
                                vastViewName.getSchemaName(),
                                transactionHandle));
                throw error;
            }
        }
        catch (final VastException e) {
            LOG.error(e,
                    format("createView: failed creating view. vastViewName=%s, tx=%s",
                            vastViewName.getSchemaName(), transactionHandle));
            throw toRuntime(e);
        }
    }

    @Override
    public void renameView(final ConnectorSession session,
                           SchemaTableName origSource,
                           SchemaTableName origTarget)
    {
        final String endUser = session.getUser();
        final SchemaTableName source = toVastSchemaTableName(session,
                origSource);
        final SchemaTableName target = toVastSchemaTableName(session,
                origTarget);
        try {
            if (!client.schemaExists(transactionHandle, source.getSchemaName(),
                    endUser)) {
                final RuntimeException error = new SchemaNotFoundException(
                        source.getSchemaName());
                LOG.debug(error,
                        "renameView: no such source schema=%s, tx=%s, endUser=%s",
                        source.getSchemaName(), transactionHandle, endUser);
                throw error;
            }
            else if (!client.viewExists(transactionHandle,
                    source.getSchemaName(), source.getTableName(), endUser)) {
                final RuntimeException error = new ViewNotFoundException(
                        source);
                LOG.debug(error,
                        "renameView: no source view=%s, tx=%s, endUser=%s",
                        source.getTableName(), transactionHandle, endUser);
                throw error;
            }
            if (!client.schemaExists(transactionHandle, target.getSchemaName(),
                    endUser)) {
                final RuntimeException error = new SchemaNotFoundException(
                        target.getSchemaName());
                LOG.debug(error,
                        "renameView: no such target schema=%s, tx=%s, endUser=%s",
                        target.getSchemaName(), transactionHandle, endUser);
                throw error;
            }
            else if (client.viewExists(transactionHandle,
                    target.getSchemaName(), target.getTableName(), endUser)) {
                final RuntimeException error = new ViewAlreadyExistsException(
                        target);
                LOG.debug(error,
                        "renameView: target view=%s already exists, tx=%s, endUser=%s",
                        target.getTableName(), transactionHandle, endUser);
                throw error;
            }
            else if (client.tableExists(transactionHandle,
                    target.getSchemaName(), target.getTableName(), endUser)) {
                final RuntimeException error = new TableAlreadyExistsException(
                        target);
                LOG.debug(error,
                        "renameView: target path=%s already exists and is a table, tx=%s, endUser=%s",
                        target.getTableName(), transactionHandle, endUser);
                throw error;
            }

            final Optional<ConnectorViewDefinition> view = getView(session,
                    source);
            final ConnectorViewDefinition definition = view.orElseThrow(() ->
            {
                final RuntimeException error = new IllegalStateException(
                        format("Cannot rename missing view: view '%s' not found",
                                source));
                LOG.error(error, "renameView: source=%s, target=%s, tx=%s",
                        source, target, transactionHandle);
                return error;
            });
            final Map<String, String> vastProperties = definition instanceof VastConnectorViewDefinition vast ?
                    vast.getProperties() :
                    new HashMap<>();
            final Map<String, Object> properties = new HashMap<>(
                    vastProperties.size());
            properties.putAll(vastProperties);
            createView(session, target, definition, properties, false);
            dropView(session, source);
        }
        catch (final VastException internal) {
            final RuntimeException error = toRuntime(internal);
            LOG.debug(error, "renameView: end rethrow VastException, tx=%s",
                    transactionHandle);
            throw error;
        }
    }

    @Override
    public void dropView(final ConnectorSession session,
                         final SchemaTableName origViewName)
    {
        final String endUser = session.getUser();
        final SchemaTableName viewName = toVastSchemaTableName(session,
                origViewName);
        try {
            if (client.viewExists(transactionHandle, viewName.getSchemaName(),
                    viewName.getTableName(), endUser)) {
                LOG.debug(
                        "dropView: dropping existing viewName=%s, tx=%s, endUser=%s",
                        viewName, transactionHandle, endUser);
                client.dropView(transactionHandle,
                        new DropViewContext(viewName.getSchemaName(),
                                viewName.getTableName()), endUser);
            }
            else {
                LOG.debug(
                        "dropView: view=%s does not exist, will not be dropped, tx=%s, endUser=%s",
                        viewName, transactionHandle, endUser);
            }
        }
        catch (final VastException error) {
            LOG.error(error,
                    "dropView: failed dropping viewName=%s, tx=%s, endUser=%s",
                    viewName, transactionHandle, endUser);
            throw new RuntimeException(error);
        }
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session,
                                                  ConnectorTableHandle tableHandle,
                                                  List<ColumnHandle> columns,
                                                  RetryMode retryMode)
    {
        LOG.debug(
                "beginInsert: tableHandle=%s, columns=%s, retryMode=%s, tx=%s",
                tableHandle, columns, retryMode, transactionHandle);
        VastTableHandle vastTableHandle = (VastTableHandle) tableHandle;
        List<VastColumnHandle> insertColumns = columns
                .stream()
                .map(VastColumnHandle.class::cast)
                .collect(toList());
        Optional<VastPartitioningHandle> partitioningHandle = vastTableHandle.getPartitioningHandle();
        if (vastTableHandle.getForImportData()) {
            Set<String> insertColumnNames = insertColumns
                    .stream()
                    .map(ch -> ch.getField().getName())
                    .collect(Collectors.toSet());
            List<String> partitionNames = vastTableHandle
                    .getPartitionPostTransformColumnNames()
                    .orElse(List.of());
            if (!insertColumnNames.containsAll(partitionNames)) {
                partitioningHandle = Optional.empty();
            }
        }
        return new VastInsertTableHandle(vastTableHandle, insertColumns, false,
                vastTableHandle.getForImportData(), partitioningHandle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
                                                          ConnectorInsertTableHandle insertHandle,
                                                          List<ConnectorTableHandle> sourceTableHandles,
                                                          Collection<Slice> fragments,
                                                          Collection<ComputedStatistics> computedStatistics)
    {
        LOG.debug(
                "finishInsert: insertHandle=%s, fragments=%s, computedStatistics=%s, tx=%s",
                insertHandle, fragments, computedStatistics, transactionHandle);
        return Optional.empty();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint constraint)
    {
        LOG.debug("applyFilter: begin: handle=%s, constraint=%s, tx=%s", handle,
                constraint, transactionHandle);
        VastTableHandle table = (VastTableHandle) handle;
        // TODO: don't push down domains on unsupported types
        TupleDomain<VastColumnHandle> summary = constraint
                .getSummary()
                .transformKeys(VastColumnHandle.class::cast);

        // We support predicates over scalar columns (including "leaf column"-only projections)
        // If only_ordered_pushdown is true, only allow pushdown on sorted columns
        BiPredicate<VastColumnHandle, Domain> isEnforcedFilterPushdown;
        if (VastSessionProperties.getOnlyOrderedPushdown(session)) {
            // Get sorted columns for this table
            List<String> sortedColumns = table
                    .getSortedColumns()
                    .orElse(Collections.emptyList());
            isEnforcedFilterPushdown = (column, domain) -> column
                    .getField()
                    .getChildren()
                    .isEmpty() && sortedColumns.contains(
                    column.getField().getName());
        }
        else {
            // Keep current behavior: scalar columns only
            isEnforcedFilterPushdown = (column, domain) -> column
                    .getField()
                    .getChildren()
                    .isEmpty();
        }
        TupleDomain<VastColumnHandle> enforcedPredicate = summary.filter(
                isEnforcedFilterPushdown);

        enforcedPredicate = table.getPredicate().intersect(enforcedPredicate);
        LOG.debug("applyFilter: tupleDomain=%s, tx=%s", enforcedPredicate,
                transactionHandle);

        ConnectorExpression connectorExpression = constraint.getExpression();
        Optional<ComplexPredicate> complexPredicate = Optional.empty();
        // TODO(ORION-107695): currently we don't support both TupleDomain & full ConnectorExpression pushdown
        // TODO(ORION-289543) - revisit the condition to allow complex predicate pushdown with partitioned tables
        if (getComplexPredicatePushdown(
                session) && enforcedPredicate.isAll() && table
                .getPartitionColumns()
                .isEmpty()) {
            VastConnectorExpressionPushdown pushdown = new VastConnectorExpressionPushdown(
                    constraint.getAssignments());
            LOG.debug("applyFilter: parsing connector expression: %s, tx=%s",
                    connectorExpression, transactionHandle);
            complexPredicate = pushdown.apply(connectorExpression);
            LOG.debug("applyFilter: parsed complex predicate: %s, tx=%s",
                    complexPredicate, transactionHandle);
            if (complexPredicate.isPresent()) {
                LOG.debug(
                        "applyFilter: set connectorExpression to Constant.TRUE, tx=%s",
                        transactionHandle);
                connectorExpression = Constant.TRUE; // pushed down successfully into VAST
            }
        }

        // TODO: refactor into VastConnectorExpressionPushdown#parse
        // If possible, parse an AND of supported LIKE expressions ("best-effort" pushdown)
        List<ConnectorExpression> conjuncts = ConnectorExpressions.extractConjuncts(
                connectorExpression);
        Consumer<ConnectorExpression> unsupportedExpressionsConsumer;
        ImmutableList.Builder<ConnectorExpression> unsupportedExpressions = ImmutableList.builder();

        if (table.getForNonAcidOperation()) {
            unsupportedExpressionsConsumer = (e) ->
            {
                throw vastTrinoExceptionFactory.fromVastException(new VastUserException(format("Non-acid operations support only a fully pushed-down predicate. pushdown not supported for: %s", e)));
            };
        }
        else {
            unsupportedExpressionsConsumer = unsupportedExpressions::add;
        }

        ImmutableList.Builder<VastSubstringMatch> substringMatchBuilder = ImmutableList.builder();
        Set<String> pushedDownColumnNames = enforcedPredicate
                .getDomains()
                .orElse(Map.of())
                .keySet()
                .stream()
                .map(col -> col.getField().getName())
                .collect(Collectors.toCollection(HashSet<String>::new));

        for (ConnectorExpression conjunct : conjuncts) {
            Optional<VastSubstringMatch> result = Optional.empty();
            if (getMatchSubstringPushdown(session)) {
                result = tryParseSubstringMatch(conjunct, pushedDownColumnNames,
                        constraint.getAssignments());
            }
            if (result.isPresent()) {
                VastSubstringMatch substringMatch = result.orElseThrow();
                substringMatchBuilder.add(
                        substringMatch); // enforced by our connector
                pushedDownColumnNames.add(substringMatch
                        .column()
                        .getField()
                        .getName()); // we support 1 LIKE per column
            }
            else {
                unsupportedExpressionsConsumer.accept(
                        conjunct); // post-filtered by Trino engine
            }
        }

        Set<VastSubstringMatch> substringMatches = new LinkedHashSet<>(
                table.getSubstringMatches());
        substringMatches.addAll(substringMatchBuilder.build());
        LOG.debug("applyFilter: substringMatches: %s, tx=%s", substringMatches,
                transactionHandle);
        if (table.getPredicate().equals(enforcedPredicate) && Objects.equals(
                Optional.ofNullable(table.getComplexPredicate()),
                complexPredicate) && new HashSet<>(
                table.getSubstringMatches()).equals(substringMatches)) {
            return Optional.empty(); // no need to update current table handle
        }
        TupleDomain<VastColumnHandle> unenforcedPredicate = summary.filter(
                isEnforcedFilterPushdown.negate());
        LOG.debug(
                "applyFilter: pushed-down predicate: enforced=%s, complex=%s, unenforced=%s, " + "matches=%s, unsupportedExpressions=%s, tx=%s",
                enforcedPredicate, complexPredicate, unenforcedPredicate,
                substringMatches, unsupportedExpressions, transactionHandle);

        VastTableHandle newTable = table.withPredicate(enforcedPredicate,
                complexPredicate, List.copyOf(substringMatches));
        return Optional.of(new ConstraintApplicationResult<>(newTable,
                unenforcedPredicate.transformKeys(ColumnHandle.class::cast),
                ConnectorExpressions.and(
                        unsupportedExpressions.build()), // unenforced expressions
                true)); // keep previously estimated statistics (over Scan+Filter nodes)
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        LOG.debug(
                "applyProjection: handle=%s, projections=%s, assignments=%s, tx=%s",
                handle, projections, assignments, transactionHandle);
        ImmutableList.Builder<ConnectorExpression> projectionsBuilder = ImmutableList.builder();
        ImmutableList.Builder<Assignment> assignmentBuilder = ImmutableList.builder();
        Set<String> newVariables = new HashSet<>(projections.size());
        VastProjectionPushdown pushdown = new VastProjectionPushdown(session);

        for (ConnectorExpression projection : projections) {
            Optional<VastProjectionPushdown.Result> expressionPushdownResult =
                    getExpressionProjectionPushdown(session) ?
                            pushdown.apply(projection) :
                            Optional.empty();
            if (expressionPushdownResult.isPresent()) {
                VastProjectionPushdown.Result result = expressionPushdownResult.orElseThrow();
                LOG.debug("applyProjection: result=%s, tx=%s", result,
                        transactionHandle);
                for (VastExpression expression : result.getPushedDown()) {
                    String variableName = expression.getVariableName();
                    VastColumnHandle column = (VastColumnHandle) requireNonNull(
                            assignments.get(variableName),
                            () -> format("Missing %s in %s", variableName,
                                    assignments));
                    String name = expression.getVariableName();
                    Type type = expression.getVariableType();
                    if (nonNull(expression.getFunction())) {
                        // handle non-identity projection
                        column = column.withProjectionExpression(expression);
                        name = expression.toString();
                        type = expression.getResultType();
                    }
                    if (newVariables.add(name)) {
                        Assignment newAssignment = new Assignment(name, column,
                                type);
                        assignmentBuilder.add(newAssignment);
                        LOG.debug(
                                "applyProjection: expressionPushdownResult: new variable=%s, assignment=%s, tx=%s",
                                name, newAssignment, transactionHandle);
                    }
                }
                projectionsBuilder.add(result.getRemaining());
                continue;
            }
            // TODO: move the below code into `ProjectionPushdown`
            // only variables and field dereferences are supported
            ImmutableList.Builder<Integer> reversedPath = ImmutableList.builder();
            Type projectionType = projection.getType();
            while (projection instanceof FieldDereference dereference) {
                projection = dereference.getTarget();
                reversedPath.add(
                        dereference.getField()); // the last index corresponds to top-most projection
            }
            if (projection instanceof Variable variable) {
                List<Integer> projectionPath = reversedPath.build().reverse();
                VastColumnHandle column = (VastColumnHandle) requireNonNull(
                        assignments.get(variable.getName()),
                        () -> format("Missing %s in %s", variable,
                                assignments));

                String newName = projectionPath.isEmpty() ?
                        variable.getName() :
                        format("%s#%s", variable.getName(), projectionPath);
                Variable newVariable = new Variable(newName, projectionType);
                projectionsBuilder.add(newVariable);
                if (newVariables.add(newName)) {
                    VastColumnHandle newColumn = column.withProjectionPath(
                            projectionPath); // create a new "synthetic" column handle to represent the projection
                    Assignment newAssignment = new Assignment(newName,
                            newColumn, projectionType);
                    assignmentBuilder.add(newAssignment);
                    LOG.debug(
                            "applyProjection: variable: new variable=%s, assignment=%s, tx=%s",
                            newVariable, newAssignment, transactionHandle);
                }
                continue;
            }
            if (projection instanceof Constant) {
                // Trino uses recursion to pushdown the children of unsupported expressions (e.g. `IN` is not supported in current version).
                // This may result in pushing down variables and literals (e.g. `SELECT x IN (1,2,3), y > 8 FROM t` will result in pushing down [`x`, `1`, `2`, `3`, `y > 8`]).
                // If we want to pushdown some expressions (e.g. `y > 8`) we also need to "pass through" the rest of the expressions into `projectionsBuilder` otherwise Trino
                // planner fails (due to https://github.com/trinodb/trino/blob/8b0c754d9d2e6c4e5ea4eed0c8c8cefb9146fcc0/core/trino-spi/src/main/java/io/trino/spi/connector/ConnectorMetadata.java#L1007-L1008).
                LOG.debug(
                        "applyProjection: keeping literal projection: %s, tx=%s",
                        projection, transactionHandle);
                projectionsBuilder.add(projection);
                continue;
            }
            if (projection instanceof Call) {
                LOG.debug(
                        "applyProjection: keeping function projection: %s, tx=%s",
                        projection, transactionHandle);
                projectionsBuilder.add(projection);

                forVariableChildren(projection, variable ->
                {
                    if (newVariables.add(variable.getName())) {
                        ColumnHandle column = requireNonNull(
                                assignments.get(variable.getName()),
                                () -> format("Missing %s in %s", variable,
                                        assignments));
                        LOG.debug(
                                "applyProjection: Call projection new variable column: %s, tx=%s",
                                column, transactionHandle);
                        assignmentBuilder.add(
                                new Assignment(variable.getName(), column,
                                        variable.getType()));
                    }
                });
                continue;
            }
            LOG.warn(
                    "applyProjection: cannot pushdown unsupported projection: %s, tx=%s",
                    projection, transactionHandle);
            return Optional.empty();
        }

        List<ConnectorExpression> newProjections = projectionsBuilder.build();
        List<Assignment> newAssignments = assignmentBuilder.build();
        if (newProjections.equals(projections)) {
            return Optional.empty(); // no change in projections
        }
        LOG.debug(
                "applyProjection: newProjections=%s, newAssignments=%s, tx=%s",
                newProjections, newAssignments, transactionHandle);
        return Optional.of(
                new ProjectionApplicationResult<>(handle, newProjections,
                        newAssignments, true));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle handle,
            long limit)
    {
        LOG.debug("applyLimit: handle=%s, limit=%s", handle, limit,
                transactionHandle);
        VastTableHandle table = (VastTableHandle) handle;
        if (table
                .getLimit()
                .map(currentLimit -> limit < currentLimit)
                .orElse(true)) {
            return Optional.of(
                    new LimitApplicationResult<>(table.withLimit(limit),
                            false /*limitGuaranteed*/,
                            true /*precalculateStatistics*/));
        }
        return Optional.empty();
    }

    public TableStatistics getTableStatistics(ConnectorSession session,
                                              ConnectorTableHandle tableHandle)
    {
        final String endUser = session.getUser();
        VastTableHandle vastTableHandle = (VastTableHandle) tableHandle;
        String tableName = vastTableHandle.getTableName();
        String schemaName = vastTableHandle.getSchemaName();
        String fullTableName = format("%s/%s", schemaName, tableName);
        LOG.debug("getTableStatistics: table url=%s, tx=%s", fullTableName,
                transactionHandle);
        Optional<TableStatistics> tableStatisticsOpt = this.statisticsManager.getTableStatistics(vastTableHandle);
        if (tableStatisticsOpt.isEmpty()) {
            LOG.debug("getTableStatistics: no analyze results for table %s, using server stats", tableName);
            try {
                VastStatistics tableStats = client.getTableStats(transactionHandle, schemaName, tableName, endUser);
                tableStatisticsOpt = Optional.of(TableStatistics.builder().setRowCount(Estimate.of(tableStats.getNumRows())).build());
            }
            catch (VastException e) {
                LOG.warn("getTableStatistics: failed to get table statistics for table %s", tableName);
                tableStatisticsOpt = Optional.of(TableStatistics.empty());
            }
        }
        return tableStatisticsOpt.get();
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Map<String, Object> analyzeProperties)
    {
        LOG.debug("getStatisticsCollectionMetadata: table=%s, tx=%s", handle,
                transactionHandle);
        Set<String> columnsToCollect = (Set<String>) analyzeProperties.get(
                "columns");
        VastTableHandle vastTableHandle = ((VastTableHandle) handle);
        List<VastColumnHandle> tableColumnHandles = vastTableHandle.getColumnHandlesCache();
        Set<VastColumnHandle> columnsToCollectSet = tableColumnHandles
                .stream()
                .filter(vch -> columnsToCollect == null || columnsToCollect.contains(
                        vch.getField().getName()))
                .collect(Collectors.toSet());
        VastTableHandle newVastTableHandle = ((VastTableHandle) handle).withAnalyzeColumns(
                columnsToCollectSet);
        return new ConnectorAnalyzeMetadata(newVastTableHandle,
                statisticsManager.getTableStatisticsMetadata(
                        columnsToCollectSet.stream()));
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session,
                                                          ConnectorTableHandle tableHandle)
    {
        LOG.debug("beginStatisticsCollection: table=%s, tx=%s", tableHandle,
                transactionHandle);
        return tableHandle;
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session,
                                           ConnectorTableHandle tableHandle,
                                           Collection<ComputedStatistics> computedStatistics)
    {
        final String endUser = session.getUser();
        int clientPageSize = VastSessionProperties.getClientPageSize(session);
        LOG.debug(
                "finishStatisticsCollection: table=%s, clientPageSize=%s, tx=%s, endUser=%s",
                tableHandle, clientPageSize, transactionHandle, endUser);

        try {
            VastTableHandle vastTableHandle = populateMetadata(
                    (VastTableHandle) tableHandle, clientPageSize, endUser);
            List<VastColumnHandle> tableColumnHandles = vastTableHandle.getColumnHandlesCache();
            ComputedStatistics allTableStatistics = Iterables.getOnlyElement(
                    computedStatistics); // there is only one per table
            this.statisticsManager.applyTableStatistics(vastTableHandle,
                    tableColumnHandles, allTableStatistics);
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
    }

    @Override
    public void renameSchema(ConnectorSession session,
                             String source,
                             String fullSchemaPath)
    {
        final String endUser = session.getUser();
        source = toVastSchemaName(session, source);
        fullSchemaPath = toVastSchemaName(session, fullSchemaPath);
        LOG.info("renameSchema: renaming schema=%s to %s, tx=%s, endUser=%s",
                source, fullSchemaPath, transactionHandle, endUser);
        String schemaName = fullSchemaPath.split("/", 2)[1];
        AlterSchemaContext ctx = new AlterSchemaContext(schemaName, null);
        try {
            client.alterSchema(transactionHandle, source, ctx, endUser);
        }
        catch (VastException e) {
            LOG.error(e,
                    "renameSchema: failed renaming schema=%s to %s, tx=%s, endUser=%s",
                    source, fullSchemaPath, transactionHandle, endUser);
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
    }

    @Override
    public void renameTable(ConnectorSession session,
                            ConnectorTableHandle tableHandle,
                            SchemaTableName newTableName)
    {
        final String endUser = session.getUser();
        newTableName = toVastSchemaTableName(session, newTableName);
        LOG.debug("renameTable: table=%s to %s, tx=%s, endUser=%s", tableHandle,
                newTableName, transactionHandle, endUser);
        VastTableHandle vastTableHandle = (VastTableHandle) tableHandle;
        String tableName = vastTableHandle.getTableName();
        String schemaName = vastTableHandle.getSchemaName();
        String newTableNameStr = newTableName.getTableName();
        String newFullSchemaNameStr = newTableName.getSchemaName();
        String oldBucketName = schemaName.split("/", 2)[0];
        String[] split = newFullSchemaNameStr.split("/", 2);
        String newBucketName = split[0];
        if (!oldBucketName.equalsIgnoreCase(newBucketName)) {
            final TrinoException error = new TrinoException(GENERIC_USER_ERROR,
                    "Changing bucket name is not supported");
            LOG.error(error,
                    "renameTable: failed renaming table=%s to %s, tx=%s)",
                    tableHandle, newTableName, transactionHandle);
            throw error;
        }
        String newSchemaName = split[1];
        String newTablePath = format("%s/%s", newSchemaName, newTableNameStr);
        try {
            AlterTableContext ctx = AlterTableContext.create(newTablePath, null,
                    null, false);
            client.alterTable(transactionHandle, schemaName, tableName, ctx,
                    endUser);
        }
        catch (VastUserException e) {
            throw vastTrinoExceptionFactory.wrapVastErrorWithInvalidPropertyError(
                    e);
        }
        catch (VastException e) {
            LOG.error(e,
                    "renameTable: Caught VastException while renaming table=%s to %s, tx=%s, endUser=%s)",
                    tableHandle, newTableName, transactionHandle, endUser);
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        catch (VastRuntimeException re) {
            LOG.error(re,
                    "renameTable: Caught VastRuntimeException while renaming table=%s to %s, tx=%s, endUser=%s)",
                    tableHandle, newTableName, transactionHandle, endUser);
            throw vastTrinoExceptionFactory.fromVastRuntimeException(re);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session,
                             ConnectorTableHandle tableHandle,
                             ColumnHandle source,
                             String target)
    {
        final String endUser = session.getUser();
        LOG.debug(
                "renameColumn: renaming column=%s of table=%s to %s, tx=%s, endUser=%s",
                source, tableHandle, target, transactionHandle, endUser);
        validateRenameColumn(tableHandle, source, target);
        VastTableHandle vastTableHandle = (VastTableHandle) tableHandle;
        String tableName = vastTableHandle.getTableName();
        String schemaName = vastTableHandle.getSchemaName();
        try {
            VastColumnHandle vastColumnHandle = (VastColumnHandle) source;
            AlterColumnContext ctx = new VastTrinoSchemaAdaptor().adaptForAlterColumn(
                    vastColumnHandle, target, null, null);
            client.alterColumn(transactionHandle, schemaName, tableName, ctx,
                    endUser);
        }
        catch (VastException e) {
            LOG.error(e,
                    "renameColumn: Caught VastException while renaming renaming column=%s of table=%s to %s, tx=%s, endUser=%s",
                    source, tableHandle, target, transactionHandle, endUser);
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        catch (VastRuntimeException re) {
            LOG.error(re,
                    "renameColumn: Caught VastRuntimeException while renaming renaming column=%s of table=%s to %s, tx=%s, endUser=%s",
                    source, tableHandle, target, transactionHandle, endUser);
            throw vastTrinoExceptionFactory.fromVastRuntimeException(re);
        }
    }

    private void validateRenameColumn(ConnectorTableHandle tableHandle,
                                      ColumnHandle source,
                                      String target)
    {
        Optional<TrinoException> exception = Optional.empty();
        if (Strings.isNullOrEmpty(target.strip())) {
            exception = Optional.of(new TrinoException(GENERIC_USER_ERROR,
                    format("Invalid target column name: %s", target)));
        }
        if (target.strip().contains(IMPORT_DATA_HIDDEN_COLUMN_NAME)) {
            exception = Optional.of(new TrinoException(GENERIC_USER_ERROR,
                    format("Target column name %s is not allowed", target)));
        }
        exception.ifPresent(e ->
        {
            LOG.error(e,
                    format("validateRenameColumn: renaming column=%s of table=%s to %s failed, tx=%s",
                            source, tableHandle, target, transactionHandle));
            throw e;
        });
    }

    @Override
    public void setTableProperties(ConnectorSession session,
                                   ConnectorTableHandle tableHandle,
                                   Map<String, Optional<Object>> properties)
    {
        final String endUser = session.getUser();
        LOG.debug(
                "setTableProperties: table=%s, properties=%s, tx=%s, endUser=%s",
                tableHandle, properties, transactionHandle, endUser);
        VastTableHandle vastTableHandle = (VastTableHandle) tableHandle;
        String tableName = vastTableHandle.getTableName();
        String schemaName = vastTableHandle.getSchemaName();
        try {
            List<String> columnNames = vastTableHandle
                    .getColumnHandlesCache()
                    .stream()
                    .map(VastColumnHandle::getField)
                    .map(Field::getName)
                    .collect(toList());
            Optional<Object> sortedBy = properties.get(SORTED_BY_PROPERTY);
            if (sortedBy != null) {
                List<String> sortedProperty = (List<String>) sortedBy.orElseThrow(
                        () -> new TrinoException(INVALID_TABLE_PROPERTY,
                                "Making a sorted table to unsorted is not supported"));
                List<String> sColumns = vastTableHandle
                        .getSortedColumns()
                        .orElse(List.of());
                if (!sColumns.isEmpty() && !sColumns.equals(sortedProperty)) {
                    throw new TrinoException(INVALID_TABLE_PROPERTY,
                            "Modifying the sorting key is not supported");
                }
            }
            Optional<Object> partitionedBy = properties.get(
                    PARTITIONED_BY_PROPERTY);
            if (partitionedBy != null) {
                List<PartitionColumnMetadata> partitionedByProperty = (List<PartitionColumnMetadata>) partitionedBy.orElseThrow(
                        () -> new TrinoException(INVALID_TABLE_PROPERTY,
                                "Making a partitionedBy table to non partitionedBy is not supported"));
                if (partitionedByProperty.isEmpty()) {
                    throw new TrinoException(INVALID_TABLE_PROPERTY,
                            format("empty %s is not supported",
                                    PARTITIONED_BY_PROPERTY));
                }
                validateColumnsList(partitionedByProperty
                        .stream()
                        .map(pcm -> pcm.sourceColumnName)
                        .toList(), columnNames, PARTITIONED_BY_PROPERTY);
                List<PartitionColumnMetadata> pColumns = vastTableHandle
                        .getPartitionColumns()
                        .orElse(List.of());
                if (!pColumns.isEmpty() && !pColumns.equals(
                        partitionedByProperty)) {
                    throw new TrinoException(INVALID_TABLE_PROPERTY,
                            "Modifying the partition key is not supported");
                }
            }
            AlterTableContext ctx = AlterTableContext.create(null, properties,
                    columnNames, false);
            client.alterTable(transactionHandle, schemaName, tableName, ctx,
                    endUser);
        }
        catch (VastUserException e) {
            throw vastTrinoExceptionFactory.wrapVastErrorWithInvalidPropertyError(
                    e);
        }
        catch (VastException e) {
            LOG.error(e,
                    format("setTableProperties: Caught VastException while setting table properties=%s for table=%s, tx=%s, endUser=%s)",
                            properties, tableHandle, transactionHandle,
                            endUser));
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        catch (VastRuntimeException re) {
            LOG.error(re,
                    format("VastRuntimeException: Caught VastException while setting table properties=%s for table=%s, tx=%s, endUser=%s)",
                            properties, tableHandle, transactionHandle,
                            endUser));
            throw vastTrinoExceptionFactory.fromVastRuntimeException(re);
        }
    }

    //Supporting Merge
    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session,
                                                  ConnectorTableHandle tableHandle)
    {
        LOG.info("getMergeRowIdColumnHandle: %s, tx=%s", tableHandle,
                transactionHandle);
        // Vast server will generate an extra "row ID" column, and Trino engine will pass it to VastMergePage#storeMergedRows
        // See https://trino.io/docs/current/develop/supporting-merge.html for details
        VastTableHandle vastTableHandle = (VastTableHandle) tableHandle;
        VastColumnHandle vastColumnHandle = VastColumnHandle.fromField(
                TrinoRowIDFieldFactory.INSTANCE.apply(vastTableHandle));
        LOG.info("getMergeRowIdColumnHandle: returning column=%s, tx=%s",
                vastColumnHandle, transactionHandle);
        return vastColumnHandle;
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session,
                                                  ConnectorTableHandle tableHandle)
    {
        return RowChangeParadigm.CHANGE_ONLY_UPDATED_COLUMNS;
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(final ConnectorSession session,
                                                final ConnectorTableHandle tableHandle,
                                                final Map<Integer, Collection<ColumnHandle>> updateCaseColumns,
                                                final RetryMode retryMode)
    {
        requireNonNull(tableHandle, "beginMerge: tableHandle is null");
        LOG.debug("beginMerge: table=%s, retryMode=%s, tx=%s", tableHandle, retryMode, transactionHandle);

        VastTableHandle table = ((VastTableHandle) tableHandle);

        if (table.isPit()) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED,
                    String.format("Row-level modifications are not supported on PIT table %s", table.getTableName()));
        }

        List<VastColumnHandle> allHandles = updateCaseColumns.values().stream()
                .flatMap(Collection::stream)
                .map(VastColumnHandle.class::cast)
                .distinct()
                .collect(toList());
        VastTableHandle resultTable = table.forMerge(
                table.getColumnHandlesCache());
        return new VastMergeTableHandle(resultTable, allHandles);
    }

    @Override
    public void finishMerge(ConnectorSession session,
                            ConnectorMergeTableHandle mergeTableHandle,
                            List<ConnectorTableHandle> sourceTableHandles,
                            Collection<Slice> fragments,
                            Collection<ComputedStatistics> computedStatistics)
    {
        LOG.debug("finishMerge, tx=%s", transactionHandle);
    }

    @Override
    public Metrics getMetrics(ConnectorSession session)
    {
        return Metrics.EMPTY;
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session,
                                                      ConnectorTableHandle handle)
    {
        VastTableHandle vastTableHandle = (VastTableHandle) handle;
        if (vastTableHandle.isPit()) {
            vastTableHandle.requireNonAcidOperationAllowed();
            return Optional.of(vastTableHandle);
        }
        return Optional.empty();
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session,
                                      ConnectorTableHandle handle)
    {
        VastTableHandle vastTableHandle = (VastTableHandle) handle;

        int clientPageSize = VastSessionProperties.getClientPageSize(session);
        String endUser = session.getUser();
        VastTableHandle populatedHandle;
        try {
            populatedHandle = populateMetadata(vastTableHandle, clientPageSize,
                    endUser);
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }

        return DropPartitions.drop(session, populatedHandle, client,
                transactionHandle, vastConfig, allocator, shapingLoggerFactory);
    }
}
