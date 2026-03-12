/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.vastdata.client.RowColumnSecurityResponse;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastVersion;
import com.vastdata.client.error.VastServerException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.tx.VastAutocommitTransaction;
import com.vastdata.trino.tx.VastTrinoTransactionHandleManager;
import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.SchemaRoutineName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.getTableNameForAPI;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.isImportDataTableName;
import static java.util.Objects.requireNonNull;

public class VastAccessControl
        implements ConnectorAccessControl
{
    private static final Logger LOG = Logger.get(VastAccessControl.class);
    private static final String CATALOG_NAME = "vast";
    private static final String INFORMATION_SCHEMA = "information_schema";

    private final VastClient client;
    private final VastTrinoConfig config;
    private final VastTrinoTransactionHandleManager transactionManager;

    @Inject
    public VastAccessControl(final VastTrinoConfig config,
                             final VastClient client,
                             final VastTrinoTransactionHandleManager transactionManager)
    {
        LOG.info("Creating VAST security access control: system=%s, hash=%s", VastVersion.SYS_VERSION, VastVersion.HASH);
        this.client = requireNonNull(client, "vast client is null");
        this.transactionManager = requireNonNull(transactionManager, "vast transaction factory is null");
        this.config = requireNonNull(config, "config is null");
        LOG.debug("Using config=%s", config);
    }

    private RowColumnSecurityResponse getRowColumnSecurity(final String schema,
                                                           final SchemaTableName schemaTableName,
                                                           final String endUser)
    {
        if (!config.isRowColumnSecurityEnabled()) {
            final String message = String.format("row_column_security is not enabled; current configuration is %s", config);
            final RuntimeException error = new UnsupportedOperationException(message);
            LOG.warn(error, "Please verify etc/catalog/vast.properties");
            throw error;
        }
        try (final VastAutocommitTransaction tx = VastAutocommitTransaction.createNewOrReuseFromEnv(transactionManager, () -> transactionManager.startTransaction(endUser), endUser)) {
            String tableName = schemaTableName.getTableName();
            if (isImportDataTableName(tableName)) {
                tableName = getTableNameForAPI(tableName);
                LOG.debug("stripping import suffix for table name security policy fetching", tableName);
            }
            return client.getRowColumnSecurity(tx, schema, tableName, config.isEndUserImpersonationEnabled() ? endUser : null);
        }
        catch (final VastServerException | VastUserException e) {
            throw toRuntime(e);
        }
    }

    public boolean isEnabled()
    {
        return config.getEnableAccessControl();
    }

    private boolean isRelevant(final SchemaTableName table)
    {
        if (config.isRowColumnSecurityEnabled()) {
            return !INFORMATION_SCHEMA.equals(table.getSchemaName());
        }
        else {
            LOG.debug("isRelevant skipping check because row-column-security is not enabled");
            return false;
        }
    }

    @Override
    public List<ViewExpression> getRowFilters(final ConnectorSecurityContext context, final SchemaTableName schemaTableName)
    {
        final String endUser = context.getIdentity().getUser();
        if (isRelevant(schemaTableName)) {
            final String schema = schemaTableName.getSchemaName();
            final RowColumnSecurityResponse policy = getRowColumnSecurity(schema, schemaTableName, endUser);
            LOG.debug("endUser=%s NOT ignoring policy=%s", endUser, policy);
            final List<ViewExpression> result = policy.getRowFilters().stream()
                    .map(filter ->
                            ViewExpression.builder()
                                    .expression(filter)
                                    .catalog(CATALOG_NAME)
                                    .schema(schema)
                                    .identity(context.getIdentity().getUser())
                                    // .setPath(List.of(new CatalogSchemaName(CATALOG_NAME, schema)))
                                    .build())
                    .toList();
            LOG.debug("endUser=%s result=%s", endUser, result);
            return result;
        }
        else {
            LOG.debug("endUser=%s ignores request because it's irrelevant: schemaTableName=%s", endUser, schemaTableName);
            return List.of();
        }
    }

    @Override
    public void checkCanShowColumns(final ConnectorSecurityContext context, final SchemaTableName table)
    {
        // TODO: check TabularListColumns policy (requires VAST server-side to expose this like row column security etc.)
    }

    @Override
    public void checkCanSelectFromColumns(final ConnectorSecurityContext context, final SchemaTableName schemaTableName, final Set<String> columns)
    {
        final String endUser = context.getIdentity().getUser();
        if (isRelevant(schemaTableName)) {
            final String schema = schemaTableName.getSchemaName();
            final RowColumnSecurityResponse policy = getRowColumnSecurity(schema, schemaTableName, endUser);
            LOG.debug("endUser=%s, policy=%s", endUser, policy);
            if (policy.getDeniedColumns().stream().anyMatch(columns::contains)) {
                LOG.debug("endUser=%s requested columns contain at least one denied column: requested columns: %s, denied columns: %s",
                        endUser, columns, policy.getDeniedColumns());
                AccessDeniedException.denySelectTable(schemaTableName.toString());
            }
            if (!policy.getAllowedColumns().isEmpty()) {
                if (!(columns.stream().allMatch(column -> policy.getAllowedColumns().contains(column) || policy.getMaskedColumns().containsKey(column)))) {
                    LOG.debug("endUser=%s at least one requested columns is not (allowed or masked): "
                            + "requested columns: %s, allowed columns: %s, masked columns: %s",
                            endUser, columns, policy.getAllowedColumns(), policy.getMaskedColumns());
                    AccessDeniedException.denySelectTable(schemaTableName.toString());
                }
            }
        }
        else {
            LOG.debug("endUser=%s ignores request because it's irrelevant: schemaTableName=%s", endUser, schemaTableName);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(final ConnectorSecurityContext context, final SchemaTableName schemaTableName, final Set<String> columns)
    {
        final String endUser = context.getIdentity().getUser();
        if (isRelevant(schemaTableName)) {
            final String schema = schemaTableName.getSchemaName();
            final RowColumnSecurityResponse policy = getRowColumnSecurity(schema, schemaTableName, endUser);
            LOG.debug("endUser=%s policy=%s", endUser, policy);
            if (policy.getDeniedColumns().stream().anyMatch(columns::contains)) {
                LOG.debug("endUser=%s requested columns contain at least one denied column: requested columns: %s, denied columns: %s", endUser, columns, policy.getDeniedColumns());
                AccessDeniedException.denyCreateViewWithSelect(schemaTableName.toString(), context.getIdentity());
            }
            if (!policy.getAllowedColumns().isEmpty()) {
                if (!(columns.stream().allMatch(column -> policy.getAllowedColumns().contains(column) || policy.getMaskedColumns().containsKey(column)))) {
                    LOG.debug("endUser=%s at least one requested columns is not (allowed or masked): requested columns: %s, allowed columns: %s, masked columns: %s", endUser, columns, policy.getAllowedColumns(), policy.getMaskedColumns());
                    AccessDeniedException.denyCreateViewWithSelect(schemaTableName.toString(), context.getIdentity());
                }
            }
        }
        else {
            LOG.debug("endUser=%s ignores request because it's irrelevant: schemaTableName=%s", endUser, schemaTableName);
        }
    }

    @Override
    public Set<String> filterSchemas(final ConnectorSecurityContext context, final Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public Set<SchemaTableName> filterTables(final ConnectorSecurityContext context, final Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(final ConnectorSecurityContext context, final Map<SchemaTableName, Set<String>> tableColumns)
    {
        return tableColumns;
    }

    @Override
    public boolean canExecuteFunction(final ConnectorSecurityContext systemSecurityContext, final SchemaRoutineName functionName)
    {
        return true;
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(final ConnectorSecurityContext systemSecurityContext, final SchemaRoutineName functionName)
    {
        return true;
    }

    @Override
    public Set<SchemaFunctionName> filterFunctions(final ConnectorSecurityContext context, final Set<SchemaFunctionName> functionNames)
    {
        return functionNames;
    }

    @Override
    public Map<ColumnSchema, ViewExpression> getColumnMasks(final ConnectorSecurityContext context, final SchemaTableName schemaTableName, final List<ColumnSchema> columns)
    {
        final String endUser = context.getIdentity().getUser();
        if (isRelevant(schemaTableName)) {
            final String schema = schemaTableName.getSchemaName();
            final RowColumnSecurityResponse policy = getRowColumnSecurity(schema, schemaTableName, endUser);
            LOG.debug("endUser=%s policy=%s", endUser, policy);
            final Function<String, ColumnSchema> fixKey = columnName ->
                    columns.stream().filter(column -> column.getName().equals(columnName))
                            .findAny().orElseThrow(() -> new IllegalStateException(String.format("Unexpected column: column = %s, columns = %s", columnName, columns)));
            final Function<String, ViewExpression> fixValue = mask -> ViewExpression.builder().expression(mask).build();
            return policy.getMaskedColumns().entrySet().stream()
                    .map(entry -> new AbstractMap.SimpleEntry<>(fixKey.apply(entry.getKey()), fixValue.apply(entry.getValue())))
                    .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        else {
            LOG.debug("endUser=%s ignores request because it's irrelevant: schemaTableName=%s", endUser, schemaTableName);
            return ImmutableMap.of();
        }
    }

    @Override
    public void checkCanCreateSchema(final ConnectorSecurityContext context, final String schema, final Map<String, Object> properties)
    {
    }

    @Override
    public void checkCanInsertIntoTable(final ConnectorSecurityContext context, final SchemaTableName table)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(final ConnectorSecurityContext context, final String propertyName)
    {
    }

    @Override
    public void checkCanShowSchemas(final ConnectorSecurityContext context)
    {
    }

    @Override
    public void checkCanShowTables(final ConnectorSecurityContext context, final String schema)
    {
    }

    @Override
    public void checkCanCreateTable(final ConnectorSecurityContext context, final SchemaTableName table, final Map<String, Object> properties)
    {
    }

    @Override
    public void checkCanDeleteFromTable(final ConnectorSecurityContext context, final SchemaTableName table)
    {
    }

    @Override
    public void checkCanUpdateTableColumns(final ConnectorSecurityContext securityContext, final SchemaTableName table, final Set<String> updatedColumnNames)
    {
    }

    @Override
    public void checkCanDropSchema(final ConnectorSecurityContext context, final String schema)
    {
    }

    @Override
    public void checkCanRenameSchema(final ConnectorSecurityContext context, final String schema, final String newSchemaName)
    {
    }

    @Override
    public void checkCanDropTable(final ConnectorSecurityContext context, final SchemaTableName table)
    {
    }

    @Override
    public void checkCanRenameColumn(final ConnectorSecurityContext context, final SchemaTableName table)
    {
    }

    @Override
    public void checkCanDropView(final ConnectorSecurityContext context, final SchemaTableName view)
    {
    }

    @Override
    public void checkCanAddColumn(final ConnectorSecurityContext context, final SchemaTableName table)
    {
    }

    @Override
    public void checkCanRenameTable(final ConnectorSecurityContext context, final SchemaTableName table, final SchemaTableName newTable)
    {
    }

    @Override
    public void checkCanDropColumn(final ConnectorSecurityContext context, final SchemaTableName table)
    {
    }

    @Override
    public void checkCanCreateView(final ConnectorSecurityContext context, final SchemaTableName view)
    {
    }

    @Override
    public void checkCanRenameView(final ConnectorSecurityContext context, final SchemaTableName view, final SchemaTableName newView)
    {
    }

    @Override
    public void checkCanShowCreateTable(final ConnectorSecurityContext context, final SchemaTableName table)
    {
    }

    @Override
    public void checkCanSetTableProperties(final ConnectorSecurityContext context, final SchemaTableName table, final Map<String, Optional<Object>> properties)
    {
    }


    /// ============= Stub super implementations with logs =============

    @Override
    public void checkCanShowCreateSchema(final ConnectorSecurityContext context, final String schemaName)
    {
        ConnectorAccessControl.super.checkCanShowCreateSchema(context, schemaName);
    }

    @Override
    public void checkCanSetTableComment(final ConnectorSecurityContext context, final SchemaTableName table)
    {
        ConnectorAccessControl.super.checkCanSetTableComment(context, table);
    }

    @Override
    public void checkCanSetViewComment(final ConnectorSecurityContext context, final SchemaTableName view)
    {
        ConnectorAccessControl.super.checkCanSetViewComment(context, view);
    }

    @Override
    public void checkCanSetColumnComment(final ConnectorSecurityContext context, final SchemaTableName table)
    {
        ConnectorAccessControl.super.checkCanSetColumnComment(context, table);
    }

    @Override
    public void checkCanAlterColumn(final ConnectorSecurityContext context, final SchemaTableName table)
    {
        ConnectorAccessControl.super.checkCanAlterColumn(context, table);
    }

    @Override
    public void checkCanTruncateTable(final ConnectorSecurityContext context, final SchemaTableName table)
    {
        ConnectorAccessControl.super.checkCanTruncateTable(context, table);
    }

    @Override
    public void checkCanCreateMaterializedView(final ConnectorSecurityContext context, final SchemaTableName materializedView, final Map<String, Object> properties)
    {
        ConnectorAccessControl.super.checkCanCreateMaterializedView(context, materializedView, properties);
    }

    @Override
    public void checkCanRefreshMaterializedView(final ConnectorSecurityContext context, final SchemaTableName materializedView)
    {
        ConnectorAccessControl.super.checkCanRefreshMaterializedView(context, materializedView);
    }

    @Override
    public void checkCanDropMaterializedView(final ConnectorSecurityContext context, final SchemaTableName materializedView)
    {
        ConnectorAccessControl.super.checkCanDropMaterializedView(context, materializedView);
    }

    @Override
    public void checkCanRenameMaterializedView(final ConnectorSecurityContext context, final SchemaTableName view, final SchemaTableName newView)
    {
        ConnectorAccessControl.super.checkCanRenameMaterializedView(context, view, newView);
    }

    @Override
    public void checkCanSetMaterializedViewProperties(final ConnectorSecurityContext context, final SchemaTableName materializedView, final Map<String, Optional<Object>> properties)
    {
        ConnectorAccessControl.super.checkCanSetMaterializedViewProperties(context, materializedView, properties);
    }

    @Override
    public void checkCanGrantSchemaPrivilege(final ConnectorSecurityContext context, final Privilege privilege, final String schema, final TrinoPrincipal grantee, final boolean grantOption)
    {
        ConnectorAccessControl.super.checkCanGrantSchemaPrivilege(context, privilege, schema, grantee, grantOption);
    }

    @Override
    public void checkCanDenySchemaPrivilege(final ConnectorSecurityContext context, final Privilege privilege, final String schema, final TrinoPrincipal grantee)
    {
        ConnectorAccessControl.super.checkCanDenySchemaPrivilege(context, privilege, schema, grantee);
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(final ConnectorSecurityContext context, final Privilege privilege, final String schema, final TrinoPrincipal revokee, final boolean grantOption)
    {
        ConnectorAccessControl.super.checkCanRevokeSchemaPrivilege(context, privilege, schema, revokee, grantOption);
    }

    @Override
    public void checkCanGrantTablePrivilege(final ConnectorSecurityContext context, final Privilege privilege, final SchemaTableName table, final TrinoPrincipal grantee, final boolean grantOption)
    {
        ConnectorAccessControl.super.checkCanGrantTablePrivilege(context, privilege, table, grantee, grantOption);
    }

    @Override
    public void checkCanDenyTablePrivilege(final ConnectorSecurityContext context, final Privilege privilege, final SchemaTableName table, final TrinoPrincipal grantee)
    {
        ConnectorAccessControl.super.checkCanDenyTablePrivilege(context, privilege, table, grantee);
    }

    @Override
    public void checkCanRevokeTablePrivilege(final ConnectorSecurityContext context, final Privilege privilege, final SchemaTableName table, final TrinoPrincipal revokee, final boolean grantOption)
    {
        ConnectorAccessControl.super.checkCanRevokeTablePrivilege(context, privilege, table, revokee, grantOption);
    }

    @Override
    public void checkCanShowRoles(final ConnectorSecurityContext context)
    {
        ConnectorAccessControl.super.checkCanShowRoles(context);
    }

    @Override
    public void checkCanCreateRole(final ConnectorSecurityContext context, final String role, final Optional<TrinoPrincipal> grantor)
    {
        ConnectorAccessControl.super.checkCanCreateRole(context, role, grantor);
    }

    @Override
    public void checkCanDropRole(final ConnectorSecurityContext context, final String role)
    {
        ConnectorAccessControl.super.checkCanDropRole(context, role);
    }

    @Override
    public void checkCanGrantRoles(final ConnectorSecurityContext context,
                                   final Set<String> roles,
                                   final Set<TrinoPrincipal> grantees,
                                   final boolean adminOption,
                                   final Optional<TrinoPrincipal> grantor)
    {
        ConnectorAccessControl.super.checkCanGrantRoles(context, roles, grantees, adminOption, grantor);
    }

    @Override
    public void checkCanRevokeRoles(final ConnectorSecurityContext context,
                                    final Set<String> roles,
                                    final Set<TrinoPrincipal> grantees,
                                    final boolean adminOption,
                                    final Optional<TrinoPrincipal> grantor)
    {
        ConnectorAccessControl.super.checkCanRevokeRoles(context, roles, grantees, adminOption, grantor);
    }

    @Override
    public void checkCanShowCurrentRoles(final ConnectorSecurityContext context)
    {
        ConnectorAccessControl.super.checkCanShowCurrentRoles(context);
    }

    @Override
    public void checkCanShowRoleGrants(final ConnectorSecurityContext context)
    {
        ConnectorAccessControl.super.checkCanShowRoleGrants(context);
    }

    @Override
    public void checkCanExecuteProcedure(final ConnectorSecurityContext systemSecurityContext, final SchemaRoutineName procedure)
    {
        ConnectorAccessControl.super.checkCanExecuteProcedure(systemSecurityContext, procedure);
    }

    @Override
    public void checkCanExecuteTableProcedure(final ConnectorSecurityContext systemSecurityContext, final SchemaTableName table, final String procedure)
    {
        ConnectorAccessControl.super.checkCanExecuteTableProcedure(systemSecurityContext, table, procedure);
    }

    @Override
    public void checkCanShowFunctions(final ConnectorSecurityContext context, final String schema)
    {
        ConnectorAccessControl.super.checkCanShowFunctions(context, schema);
    }

    @Override
    public void checkCanCreateFunction(final ConnectorSecurityContext systemSecurityContext, final SchemaRoutineName functionName)
    {
        ConnectorAccessControl.super.checkCanCreateFunction(systemSecurityContext, functionName);
    }

    @Override
    public void checkCanDropFunction(final ConnectorSecurityContext systemSecurityContext, final SchemaRoutineName functionName)
    {
        ConnectorAccessControl.super.checkCanDropFunction(systemSecurityContext, functionName);
    }

    @Override
    public void checkCanShowCreateFunction(final ConnectorSecurityContext systemSecurityContext, final SchemaRoutineName functionName)
    {
        ConnectorAccessControl.super.checkCanShowCreateFunction(systemSecurityContext, functionName);
    }
}
