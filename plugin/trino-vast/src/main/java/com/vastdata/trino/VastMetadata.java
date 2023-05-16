/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.client.error.VastServerException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.AlterColumnContext;
import com.vastdata.client.schema.AlterSchemaContext;
import com.vastdata.client.schema.AlterTableContext;
import com.vastdata.client.schema.CreateTableContext;
import com.vastdata.client.schema.DropTableContext;
import com.vastdata.client.schema.TableColumnLifecycleContext;
import com.vastdata.client.schema.VastMetadataUtils;
import com.vastdata.trino.expression.VastProjectionPushdown;
import com.vastdata.trino.expression.VastExpression;
import com.vastdata.trino.statistics.VastStatisticsManager;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.vastdata.client.importdata.VastImportDataMetadataUtils.BIG_CATALOG_TABLE_NAME;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.IMPORT_DATA_HIDDEN_COLUMN_NAME;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.IMPORT_DATA_HIDDEN_FIELD;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.getBigCatalogSearchPath;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.getTableNameForAPI;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.isImportDataTableName;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD;
import static com.vastdata.trino.VastSessionProperties.getComplexPredicatePushdown;
import static com.vastdata.trino.VastSessionProperties.getExprsesionProjectionPushdown;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.expression.StandardFunctions.LIKE_PATTERN_FUNCTION_NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class VastMetadata
        implements ConnectorMetadata
{
    public static final VastColumnHandle IMPORT_DATA_HIDDEN_COLUMN_HANDLE = new VastColumnHandle(IMPORT_DATA_HIDDEN_FIELD);
    private static final Logger LOG = Logger.get(VastMetadata.class);
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";

    private final VastClient client;
    private final VastTransactionHandle transactionHandle;
    private final VastMetadataUtils util = new VastMetadataUtils();
    private final VastTrinoExceptionFactory vastTrinoExceptionFactory = new VastTrinoExceptionFactory();
    private final VastStatisticsManager statisticsManager;

    public VastMetadata(VastClient client, VastTransactionHandle transactionHandle, VastStatisticsManager statisticsManager)
    {
        this.client = client;
        this.transactionHandle = transactionHandle;
        this.statisticsManager = statisticsManager;
    }

    @Override
    public boolean supportsMissingColumnsOnInsert()
    {
        return true;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        int clientPageSize = VastSessionProperties.getClientPageSize(session);
        LOG.debug("tx %s: listSchemaNames(%s)", transactionHandle, clientPageSize);
        try {
            return client.listAllSchemas(transactionHandle, clientPageSize).collect(Collectors.toList());
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
        LOG.debug("tx %s: schemaExists(%s)", transactionHandle, schemaName);
        try {
            return client.schemaExists(transactionHandle, schemaName);
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        catch (VastRuntimeException re) {
            throw vastTrinoExceptionFactory.fromVastRuntimeException(re);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        LOG.debug("tx %s: getTableHandle(%s)", transactionHandle, schemaTableName);
        Optional<String> bigCatalogSearchPath = getBigCatalogSearchPath(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        if (bigCatalogSearchPath.isPresent()) {
            VastTableHandle tableHandle = (VastTableHandle) getTableHandle(session, new SchemaTableName(schemaTableName.getSchemaName(), BIG_CATALOG_TABLE_NAME));
            if (tableHandle != null) {
                return tableHandle.withBigCatalogSearchPath(bigCatalogSearchPath.get());
            }
            else {
                throw new IllegalStateException(format("Table handle for Big Catalog was not found: %s", schemaTableName));
            }
        }
        String origTableName = schemaTableName.getTableName();
        String tableNameForExistenceCheck = getTableNameForAPI(origTableName);
        try {
            if (client.tableExists(transactionHandle, schemaTableName.getSchemaName(), tableNameForExistenceCheck)) {

                return new VastTableHandle(schemaTableName.getSchemaName(), origTableName, !origTableName.equals(tableNameForExistenceCheck));
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
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LOG.debug("tx %s: getTableSchema(%s)", transactionHandle, tableHandle);
        int clientPageSize = VastSessionProperties.getClientPageSize(session);
        try {
            VastTableHandle table = (VastTableHandle) tableHandle;
            SchemaTableName schemaTableName = table.toSchemaTableName();
            List<ColumnSchema> columns = getVastColumnHandles(table, clientPageSize).stream()
                    .map(VastColumnHandle::getColumnSchema)
                    .collect(Collectors.toList());
            return new ConnectorTableSchema(schemaTableName, columns);
        }
        catch (VastException e) {
            LOG.error(e, "tx %s: getTableSchema() failed: %s", transactionHandle, e);
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
    }

    private List<VastColumnHandle> getVastColumnHandles(VastTableHandle table, int pageSize)
            throws VastException
    {
        if (table.getColumnHandlesCache() == null) {
            String schemaName = table.getSchemaName();
            String tableName = table.getTableName();
            table.setColumnHandlesCache(listTableColumns(schemaName, tableName, pageSize));
        }

        if (!table.getForImportData()) {
            return table.getColumnHandlesCache();
        }
        else {
            List<VastColumnHandle> columnHandles = new ArrayList<>();
            columnHandles.add(IMPORT_DATA_HIDDEN_COLUMN_HANDLE);
            columnHandles.addAll(table.getColumnHandlesCache());
            return columnHandles;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        int clientPageSize = VastSessionProperties.getClientPageSize(session);
        try {
            LOG.debug("tx %s: getTableMetadata(%s, %s)", transactionHandle, tableHandle, clientPageSize);
            VastTableHandle table = (VastTableHandle) tableHandle;
            List<ColumnMetadata> columns = getVastColumnHandles(table, clientPageSize).stream()
                    .map(VastColumnHandle::getColumnMetadata)
                    .collect(Collectors.toList());
            ConnectorTableMetadata result = new ConnectorTableMetadata(table.toSchemaTableName(), columns);
            LOG.debug("%s", result);
            return result;
        }
        catch (VastException e) {
            LOG.error(e, "tx %s: getTableMetadata() failed: %s", transactionHandle, e);
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        int clientPageSize = VastSessionProperties.getClientPageSize(session);
        LOG.debug("tx %s: listTables(%s, %s)", transactionHandle, optionalSchemaName, clientPageSize);
        return optionalSchemaName
                .map(schemaName -> {
                    if (schemaName.equalsIgnoreCase(INFORMATION_SCHEMA_NAME)) {
                        // TODO https://github.com/trinodb/trino/issues/1559 this should be filtered out in engine.
                        return Stream.<SchemaTableName>of();
                    }
                    try {
                        return client.listTables(transactionHandle, schemaName, clientPageSize).map(tableName -> new SchemaTableName(schemaName, tableName));
                    }
                    catch (VastServerException e) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
                    }
                    catch (VastUserException e) {
                        throw new TrinoException(GENERIC_USER_ERROR, e);
                    }
                })
                .orElseGet(() -> {
                    // TODO: support listing all tables
                    LOG.warn("cannot list tables without specifying schema");
                    return Stream.empty();
                })
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        int clientPageSize = VastSessionProperties.getClientPageSize(session);
        LOG.debug("tx %s: getColumnHandles(%s, %s)", transactionHandle, tableHandle, clientPageSize);
        try {
            VastTableHandle table = (VastTableHandle) tableHandle;
            Map<String, ColumnHandle> result = getVastColumnHandles(table, clientPageSize).stream()
                    .collect(Collectors.toMap(col -> col.getField().getName(), Function.identity()));
            LOG.debug("%s", result);
            return result;
        }
        catch (VastException e) {
            LOG.error(e, "tx %s: getColumnHandles() failed: %s", transactionHandle, e);
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
    }

    private Stream<VastColumnHandle> streamTableColumnHandles(String schemaName, String tableName, boolean addImportDataPathColumn, int pageSize)
            throws VastException
    {
        List<VastColumnHandle> tableColumnsHandlesList = listTableColumns(schemaName, tableName, pageSize);
        if (addImportDataPathColumn) {
            ArrayList<VastColumnHandle> vastColumnHandlesWithImportDataColumn = new ArrayList<>();
            vastColumnHandlesWithImportDataColumn.add(IMPORT_DATA_HIDDEN_COLUMN_HANDLE);
            vastColumnHandlesWithImportDataColumn.addAll(tableColumnsHandlesList);
            return vastColumnHandlesWithImportDataColumn.stream();
        }
        else {
            return tableColumnsHandlesList.stream();
        }
    }

    private List<VastColumnHandle> listTableColumns(String schemaName, String tableName, int pageSize)
            throws VastException
    {
        if (schemaName.equalsIgnoreCase(INFORMATION_SCHEMA_NAME)) {
            // TODO https://github.com/trinodb/trino/issues/1559 this should be filtered out in engine.
            return List.of();
        }
        LOG.debug("tx %s: listTableColumns(%s/%s)", transactionHandle, schemaName, tableName);
        String tableNameForAPI = getTableNameForAPI(tableName);
        List<Field> fields = client.listColumns(transactionHandle, schemaName, tableNameForAPI, pageSize);
        return fields.stream().map(VastColumnHandle::fromField).collect(Collectors.toList());
    }

    @Override
    public Stream<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        int clientPageSize = VastSessionProperties.getClientPageSize(session);
        LOG.debug("tx %s: streamTableColumns(%s, %s)", transactionHandle, prefix, clientPageSize);
        return prefix
                .toOptionalSchemaTableName()
                .map(schemaTableName -> {
                    try {
                        String schemaName = schemaTableName.getSchemaName();
                        String tableName = schemaTableName.getTableName();
                        List<ColumnMetadata> columns = streamTableColumnHandles(schemaName, tableName, false, clientPageSize)
                                .map(VastColumnHandle::getColumnMetadata)
                                .collect(Collectors.toList());
                        LOG.debug("%s: %s", schemaTableName, columns);
                        return Stream.of(new TableColumnsMetadata(schemaTableName, Optional.of(columns)));
                    }
                    catch (VastException e) {
                        LOG.error(e, "tx %s: streamTableColumns() failed: %s", transactionHandle, e);
                        throw vastTrinoExceptionFactory.fromVastException(e);
                    }
                })
                .orElseGet(() -> {
                    // TODO: support prefix search
                    LOG.warn("schemaTableName must be specified");
                    return Stream.empty();
                });
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        LOG.debug("tx %s: getColumnMetadata(%s, %s)", transactionHandle, tableHandle, columnHandle);
        return ((VastColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        LOG.debug("tx %s: getTableProperties(%s)", transactionHandle, table);
        return new ConnectorTableProperties();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        LOG.debug("Adding column %s to table %s", column, tableHandle);
        String name = column.getName();
        if (name.equals(IMPORT_DATA_HIDDEN_COLUMN_NAME)) {
            throw new TrinoException(GENERIC_USER_ERROR, format("Illegal name for add column: %s", name));
        }
        TableColumnLifecycleContext ctx = new VastTrinoSchemaAdaptor().adaptForAddColumn(tableHandle, column);
        try {
            client.addColumn(transactionHandle, ctx);
            VastTableHandle table = (VastTableHandle) tableHandle;
            table.clearColumnHandlesCache();
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        LOG.debug("Dropping column %s of table %s", column, tableHandle);
        TableColumnLifecycleContext ctx = new VastTrinoSchemaAdaptor().adaptForDropColumn(tableHandle, column);
        try {
            client.dropColumn(transactionHandle, ctx);
            VastTableHandle table = (VastTableHandle) tableHandle;
            table.clearColumnHandlesCache();
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LOG.debug("Dropping table %s", tableHandle);
        DropTableContext ctx = new VastTrinoSchemaAdaptor().adaptForDropTable(tableHandle);
        try {
            client.dropTable(transactionHandle, ctx);
            VastTableHandle table = (VastTableHandle) tableHandle;
            table.clearColumnHandlesCache();
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        LOG.debug("Dropping schema %s", schemaName);
        try {
            client.dropSchema(transactionHandle, schemaName);
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        catch (VastRuntimeException e) {
            throw vastTrinoExceptionFactory.fromVastRuntimeException(e);
        }
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        String serializedProperties = util.getPropertiesString(properties);
        LOG.info("tx %s: Creating schema %s, with properties: %s, owner: %s", transactionHandle, schemaName, serializedProperties, owner);
        try {
            client.createSchema(transactionHandle, schemaName, serializedProperties);
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        catch (VastRuntimeException e) {
            throw vastTrinoExceptionFactory.fromVastRuntimeException(e);
        }
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        LOG.debug("tx %s: createTable(%s, ignoreExisting=%s)", transactionHandle, tableMetadata, ignoreExisting);
        String tableName = tableMetadata.getTable().getTableName();
        if (isImportDataTableName(tableName)) {
            throw new TrinoException(GENERIC_USER_ERROR, format("Illegal table name for create table: %s", tableName));
        }
        if (ignoreExisting) {
            ConnectorTableHandle table = getTableHandle(session, tableMetadata.getTable());
            if (Objects.nonNull(table)) {
                LOG.info("Table %s already exists", table);
                return;
            }
        }
        try {
            CreateTableContext ctx = new VastTrinoSchemaAdaptor().adaptForCreateTable(tableMetadata);
            client.createTable(transactionHandle, ctx);
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        catch (VastRuntimeException e) {
            throw vastTrinoExceptionFactory.fromVastRuntimeException(e);
        }
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        int clientPageSize = VastSessionProperties.getClientPageSize(session);
        LOG.debug("tx %s: beginCreateTable(%s, %s, %s, %s)", transactionHandle, tableMetadata, layout, retryMode, clientPageSize);
        try {
            createTable(session, tableMetadata, false);
            VastTableHandle table = (VastTableHandle) getTableHandle(session, tableMetadata.getTable());
            if (table != null) {
                String schemaName = table.getSchemaName();
                String tableName = table.getTableName();
                List<VastColumnHandle> columns = streamTableColumnHandles(schemaName, tableName, table.getForImportData(), clientPageSize)
                        .collect(Collectors.toList());
                return new VastInsertTableHandle(table, columns, true, false); // used for `CREATE TABLE t AS SELECT ...`
            }
            else {
                throw new TrinoException(TABLE_NOT_FOUND, format("Table doesn't exist: %s", tableMetadata));
            }
        }
        catch (VastException e) {
            LOG.error(e, "tx %s: beginCreateTable() failed: %s", transactionHandle, e);
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        LOG.debug("tx %s: finishCreateTable(%s, %s, %s)", transactionHandle, tableHandle, fragments, computedStatistics);
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        LOG.debug("tx %s: beginInsert(%s, %s, %s)", transactionHandle, tableHandle, columns, retryMode);
        VastTableHandle vastTableHandle = (VastTableHandle) tableHandle;
        return new VastInsertTableHandle(vastTableHandle,
                columns.stream().map(VastColumnHandle.class::cast).collect(Collectors.toList()),
                false, vastTableHandle.getForImportData());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        LOG.debug("tx %s: finishInsert(%s, %s, %s)", transactionHandle, insertHandle, fragments, computedStatistics);
        return Optional.empty();
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LOG.debug("tx %s: getDeleteRowIdColumnHandle(%s)", transactionHandle, tableHandle);
        // Vast server will generate an extra "row ID" column, and Trino engine will pass it to VastUpdatablePageSource#deleteRows
        // See https://trino.io/docs/current/develop/delete-and-update.html for details
        return VastColumnHandle.fromField(ROW_ID_FIELD);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        LOG.debug("tx %s: beginDelete(%s, %s)", transactionHandle, tableHandle, retryMode);
        VastTableHandle table = (VastTableHandle) tableHandle;
        // Mark the table handle as "updatable", so VastUpdatablePageSource instance will be created by VastPageSourceProvider
        return table.forDelete();
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        LOG.debug("tx %s: finishDelete(%s, %s)", transactionHandle, tableHandle, fragments);
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        LOG.debug("tx %s: getUpdateRowIdColumnHandle(%s, %s)", transactionHandle, tableHandle, updatedColumns);
        // Vast server will generate an extra "row ID" column, and Trino engine will pass it to VastUpdatablePageSource#deleteRows
        // See https://trino.io/docs/current/develop/delete-and-update.html for details
        return VastColumnHandle.fromField(ROW_ID_FIELD);
    }

    @Override
    public ConnectorTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns, RetryMode retryMode)
    {
        LOG.debug("tx %s: beginUpdate(%s, %s)", transactionHandle, tableHandle, updatedColumns, retryMode);
        VastTableHandle table = (VastTableHandle) tableHandle;
        List<VastColumnHandle> vastColumns = updatedColumns.stream().map(VastColumnHandle.class::cast).collect(Collectors.toList());
        // Mark the table handle as "updatable", so VastUpdatablePageSource instance will be created by VastPageSourceProvider
        return table.forUpdate(vastColumns);
    }

    @Override
    public void finishUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        LOG.debug("tx %s: finishUpdate(%s, %s)", transactionHandle, tableHandle, fragments);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        LOG.debug("applyFilter(%s, %s)", handle, constraint);
        VastTableHandle table = (VastTableHandle) handle;
        // TODO: don't push down domains on unsupported types
        TupleDomain<VastColumnHandle> summary = constraint.getSummary().transformKeys(VastColumnHandle.class::cast);

        // We support predicates over scalar columns (including "leaf column"-only projections)
        BiPredicate<VastColumnHandle, Domain> isEnforcedFilterPushdown = (column, domain) -> column.getField().getChildren().isEmpty();
        TupleDomain<VastColumnHandle> enforcedPredicate = summary.filter(isEnforcedFilterPushdown);

        enforcedPredicate = table.getPredicate().intersect(enforcedPredicate);
        LOG.debug("tupleDomain=%s", enforcedPredicate);

        // If possible, parse an AND of supported LIKE expressions ("best-effort" pushdown)
        List<ConnectorExpression> conjuncts = ConnectorExpressions.extractConjuncts(constraint.getExpression());
        ImmutableList.Builder<ConnectorExpression> unsupportedExpressions = ImmutableList.builder();
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
            if (getComplexPredicatePushdown(session)) {
                result = tryParseSubstringMatch(conjunct, pushedDownColumnNames, constraint.getAssignments());
            }
            if (result.isPresent()) {
                VastSubstringMatch substringMatch = result.get();
                substringMatchBuilder.add(substringMatch); // enforced by our connector
                pushedDownColumnNames.add(substringMatch.getColumn().getField().getName()); // we support 1 LIKE per column
            }
            else {
                unsupportedExpressions.add(conjunct); // post-filtered by Trino engine
            }
        }
        List<VastSubstringMatch> substringMatches = substringMatchBuilder.build();
        LOG.debug("substringMatches: %s", substringMatches);
        if (table.getPredicate().equals(enforcedPredicate) && table.getSubstringMatches().equals(substringMatches)) {
            return Optional.empty(); // no need to update current table handle
        }
        TupleDomain<VastColumnHandle> unenforcedPredicate = summary.filter(isEnforcedFilterPushdown.negate());

        LOG.debug("pushed-down predicate: enforced=%s, unenforced=%s matches=%s", enforcedPredicate, unenforcedPredicate, substringMatches);
        VastTableHandle newTable = table.withPredicate(enforcedPredicate, substringMatches);
        return Optional.of(new ConstraintApplicationResult<>(
                newTable,
                unenforcedPredicate.transformKeys(ColumnHandle.class::cast),
                ConnectorExpressions.and(unsupportedExpressions.build()), // unenforced expressions
                true)); // keep previously estimated statistics (over Scan+Filter nodes)
    }

    static Optional<VastSubstringMatch> tryParseSubstringMatch(ConnectorExpression conjunct, Set<String> pushedDownColumnNames, Map<String, ColumnHandle> assignments)
    {
        if (!(conjunct instanceof Call)) {
            return Optional.empty();
        }
        Call call = (Call) conjunct;
        if (!call.getFunctionName().equals(LIKE_PATTERN_FUNCTION_NAME)) {
            return Optional.empty();
        }
        List<ConnectorExpression> args = call.getArguments();
        if (args.size() != 2) {
            return Optional.empty();  // no support for escaped LIKE expression
        }
        Variable variable = (Variable) args.get(0);
        if (pushedDownColumnNames.contains(variable.getName())) {
            return Optional.empty(); // no support for AND between TupleDomain and ConnectorExpression on the same column
        }
        Constant constant = (Constant) args.get(1);
        Slice slice = (Slice) constant.getValue();
        String pattern = slice.toStringUtf8();
        if (!(pattern.startsWith("%") && pattern.endsWith("%") && pattern.length() > 2)) {
            return Optional.empty(); // handle only "substring" LIKE expressions
        }
        // remove leading and trailing wildcards
        String substring = pattern.substring(1, pattern.length() - 1);
        if (substring.contains("%") || substring.contains("_") || substring.contains("\\")) {
            return Optional.empty(); // no support for inner wildcards or escaping (for simplicity)
        }
        VastColumnHandle column = (VastColumnHandle) requireNonNull(assignments.get(variable.getName()),
                () -> format("missing %s in %s", variable, assignments));
        return Optional.of(new VastSubstringMatch(column, substring));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        LOG.debug("applyProjection(%s, %s, %s)", handle, projections, assignments);
        ImmutableList.Builder<ConnectorExpression> projectionsBuilder = ImmutableList.builder();
        ImmutableList.Builder<Assignment> assignmentBuilder = ImmutableList.builder();
        Set<String> newVariables = new HashSet<>(projections.size());
        VastProjectionPushdown pushdown = new VastProjectionPushdown(session);

        for (ConnectorExpression projection : projections) {
            Optional<VastProjectionPushdown.Result> expressionPushdownResult = getExprsesionProjectionPushdown(session) ? pushdown.apply(projection) : Optional.empty();
            if (expressionPushdownResult.isPresent()) {
                VastProjectionPushdown.Result result = expressionPushdownResult.get();
                LOG.debug("applyProjection: result=%s", result);
                for (VastExpression expression : result.getPushedDown()) {
                    String variableName = expression.getVariableName();
                    VastColumnHandle column = (VastColumnHandle) requireNonNull(assignments.get(variableName), () -> format("Missing %s in %s", variableName, assignments));
                    VastColumnHandle newColumn = column.withProjectionExpression(expression);
                    String newName = expression.toString();
                    if (newVariables.add(newName)) {
                        Assignment newAssignment = new Assignment(newName, newColumn, expression.getResultType());
                        assignmentBuilder.add(newAssignment);
                        LOG.debug("applyProjection: new variable=%s, assignment=%s", newName, newAssignment);
                    }
                }
                projectionsBuilder.add(result.getRemaining());
                continue;
            }
            // TODO: move the below code into `ProjectionPushdown`
            // only variables and field dereferences are supported
            ImmutableList.Builder<Integer> reversedPath = ImmutableList.builder();
            Type projectionType = projection.getType();
            while (projection instanceof FieldDereference) {
                FieldDereference dereference = (FieldDereference) projection;
                projection = dereference.getTarget();
                reversedPath.add(dereference.getField()); // the last index corresponds to top-most projection
            }
            if (projection instanceof Variable) {
                Variable variable = (Variable) projection;
                List<Integer> projectionPath = reversedPath.build().reverse();
                VastColumnHandle column = (VastColumnHandle) requireNonNull(assignments.get(variable.getName()), () -> format("Missing %s in %s", variable, assignments));

                String newName = projectionPath.isEmpty() ? variable.getName() : format("%s#%s", variable.getName(), projectionPath);
                Variable newVariable = new Variable(newName, projectionType);
                projectionsBuilder.add(newVariable);

                VastColumnHandle newColumn = column.withProjectionPath(projectionPath); // create a new "synthetic" column handle to represent the projection
                Assignment newAssignment = new Assignment(newName, newColumn, projectionType);
                assignmentBuilder.add(newAssignment);
                LOG.debug("applyProjection: new variable=%s, assignment=%s", newVariable, newAssignment);
                continue;
            }
            if (projection instanceof Constant) {
                // Trino uses recursion to pushdown the children of unsupported expressions (e.g. `IN` is not supported in current version).
                // This may result in pushing down variables and literals (e.g. `SELECT x IN (1,2,3), y > 8 FROM t` will result in pushing down [`x`, `1`, `2`, `3`, `y > 8`]).
                // If we want to pushdown some expressions (e.g. `y > 8`) we also need to "pass through" the rest of the expressions into `projectionsBuilder` otherwise Trino
                // planner fails (due to https://github.com/trinodb/trino/blob/8b0c754d9d2e6c4e5ea4eed0c8c8cefb9146fcc0/core/trino-spi/src/main/java/io/trino/spi/connector/ConnectorMetadata.java#L1007-L1008).
                LOG.debug("keeping literal projection: %s", projection);
                projectionsBuilder.add(projection);
                continue;
            }
            LOG.warn("cannot pushdown unsupported projection: %s", projection);
            return Optional.empty();
        }

        List<ConnectorExpression> newProjections = projectionsBuilder.build();
        List<Assignment> newAssignments = assignmentBuilder.build();
        if (newProjections.equals(projections)) {
            return Optional.empty(); // no change in projections
        }
        LOG.debug("applyProjection: newProjections=%s, newAssignments=%s", newProjections, newAssignments);
        return Optional.of(new ProjectionApplicationResult<>(handle, newProjections, newAssignments, true));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        LOG.debug("tx %s: applyLimit(%s, %s)", transactionHandle, handle, limit);
        VastTableHandle table = (VastTableHandle) handle;
        if (table.getLimit().map(currentLimit -> limit < currentLimit).orElse(true)) {
            return Optional.of(new LimitApplicationResult<>(table.withLimit(limit), false /*limitGuaranteed*/, true /*precalculateStatistics*/));
        }
        return Optional.empty();
    }

    @Override
    public ConnectorTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        LOG.debug("tx %s: getTableHandleForStatisticsCollection for table %s with properties %s, %s", transactionHandle, tableName, analyzeProperties, session);
        return getTableHandle(session, tableName);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        VastTableHandle vastTableHandle = (VastTableHandle) tableHandle;
        String tableName = vastTableHandle.getTableName();
        String schemaName = vastTableHandle.getSchemaName();
        String fullTableName = format("%s/%s", schemaName, tableName);
        LOG.debug("tx %s: getTableStatistics for table url %s with constraint %s, %s", transactionHandle, fullTableName, constraint, session);
        return this.statisticsManager.getTableStatistics(transactionHandle, fullTableName).orElse(TableStatistics.empty());
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        LOG.debug("tx %s: getStatisticsCollectionMetadata for table %s, %s", transactionHandle, tableMetadata, session);
        Stream<ColumnMetadata> columns = tableMetadata
                .getColumns()
                .stream()
                .filter(columnMetadata -> !columnMetadata.isHidden()); // we don't collect statistics over hidden columns
        return this.statisticsManager.getTableStatisticsMetadata(columns);
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LOG.debug("tx %s: beginStatisticsCollection for table %s, %s", transactionHandle, tableHandle, session);
        return tableHandle;
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        int clientPageSize = VastSessionProperties.getClientPageSize(session);
        LOG.debug("tx %s: finishStatisticsCollection for table %s, %s. clientPageSize: %s", transactionHandle, tableHandle, session, clientPageSize);
        VastTableHandle vastTableHandle = (VastTableHandle) tableHandle;
        System.out.println("VastTableHandle" + vastTableHandle);
        String tableName = vastTableHandle.getTableName();
        String schemaName = vastTableHandle.getSchemaName();
        try {
            List<Field> tableColumnFields = client.listColumns(this.transactionHandle, schemaName, tableName, clientPageSize);
            ComputedStatistics allTableStatistics = Iterables.getOnlyElement(computedStatistics); // there is only one per table
            this.statisticsManager.applyTableStatistics(this.transactionHandle, schemaName, tableName, tableColumnFields, allTableStatistics);
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String fullSchemaPath)
    {
        LOG.info("tx %s: Renaming schema %s to %s", transactionHandle, source, fullSchemaPath);
        String schemaName = fullSchemaPath.split("/", 2)[1];
        AlterSchemaContext ctx = new AlterSchemaContext(schemaName, null);
        try {
            client.alterSchema(transactionHandle, source, ctx);
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        LOG.debug("tx %s: renameTable table %s to %s, %s", transactionHandle, tableHandle, newTableName, session);
        VastTableHandle vastTableHandle = (VastTableHandle) tableHandle;
        String tableName = vastTableHandle.getTableName();
        String schemaName = vastTableHandle.getSchemaName();
        String newTableNameStr = newTableName.getTableName();
        String newFullSchemaNameStr = newTableName.getSchemaName();
        String oldBucketName = schemaName.split("/", 2)[0];
        String[] split = newFullSchemaNameStr.split("/", 2);
        String newBucketName = split[0];
        if (!oldBucketName.equalsIgnoreCase(newBucketName)) {
            throw new TrinoException(GENERIC_USER_ERROR, "Changing bucket name is not supported");
        }
        String newSchemaName = split[1];
        String newTablePath = format("%s/%s", newSchemaName, newTableNameStr);
        try {
            AlterTableContext ctx = new AlterTableContext(newTablePath, null);
            client.alterTable(transactionHandle, schemaName, tableName, ctx);
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        catch (VastRuntimeException re) {
            throw vastTrinoExceptionFactory.fromVastRuntimeException(re);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        LOG.debug("tx %s: renameColumn %s of table %s to %s, %s", transactionHandle, source, tableHandle, target, session);
        validateRenameColumn(tableHandle, source, target);
        VastTableHandle vastTableHandle = (VastTableHandle) tableHandle;
        String tableName = vastTableHandle.getTableName();
        String schemaName = vastTableHandle.getSchemaName();
        try {
            VastColumnHandle vastColumnHandle = (VastColumnHandle) source;
            AlterColumnContext ctx = new VastTrinoSchemaAdaptor().adaptForAlterColumn(vastColumnHandle, target, null, null);
            client.alterColumn(transactionHandle, schemaName, tableName, ctx);
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        catch (VastRuntimeException re) {
            throw vastTrinoExceptionFactory.fromVastRuntimeException(re);
        }
    }

    private void validateRenameColumn(ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        Optional<TrinoException> exception = Optional.empty();
        if (Strings.isNullOrEmpty(target.strip())) {
            exception = Optional.of(new TrinoException(GENERIC_USER_ERROR, format("Invalid target column name: %s", target)));
        }
        if (target.strip().contains(IMPORT_DATA_HIDDEN_COLUMN_NAME)) {
            exception = Optional.of(new TrinoException(GENERIC_USER_ERROR, format("Target column name %s is not allowed", target)));
        }
        exception.ifPresent(e -> {
            LOG.error(e, format("tx %s: renameColumn %s of table %s to %s failed", transactionHandle, source, tableHandle, target));
            throw e;
        });
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        LOG.debug("tx %s: setTableProperties for table %s, %s, %s", transactionHandle, tableHandle, properties, session);
        VastTableHandle vastTableHandle = (VastTableHandle) tableHandle;
        String tableName = vastTableHandle.getTableName();
        String schemaName = vastTableHandle.getSchemaName();
        try {
            AlterTableContext ctx = new AlterTableContext(null, properties);
            client.alterTable(transactionHandle, schemaName, tableName, ctx);
        }
        catch (VastException e) {
            throw vastTrinoExceptionFactory.fromVastException(e);
        }
        catch (VastRuntimeException re) {
            throw vastTrinoExceptionFactory.fromVastRuntimeException(re);
        }
    }
}
