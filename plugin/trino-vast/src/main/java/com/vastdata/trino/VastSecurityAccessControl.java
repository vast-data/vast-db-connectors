/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastVersion;
import com.vastdata.client.error.VastServerException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.RowColumnSecurityResponse;
import com.vastdata.trino.tx.VastTrinoTransactionHandleManager;
import io.airlift.log.Logger;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.QueryId;
import io.trino.spi.connector.*;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.*;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.Type;

import java.security.Principal;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class VastSecurityAccessControl
        implements SystemAccessControl
{
    private static final Logger LOG = Logger.get(VastSecurityAccessControl.class);
    private static final String CATALOG_NAME = "vast";
    private static final String TEST_USER = "testing";
    private static final String INFORMATION_SCHEMA = "information_schema";

    private final VastClient client;
    private final VastTrinoTransactionHandleManager transManager;
    private final VastPageSourceProvider pageSourceProvider;
    private final VastPageSinkProvider pageSinkProvider;
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public VastSecurityAccessControl(
            final VastClient client,
            final VastTrinoTransactionHandleManager transManager,
            final VastPageSourceProvider pageSourceProvider,
            final VastPageSinkProvider pageSinkProvider,
            final Set<SessionPropertiesProvider> sessionProperties)
    {
        LOG.info("Creating VAST security access control: system=%s, hash=%s", VastVersion.SYS_VERSION, VastVersion.HASH);
        this.client = requireNonNull(client, "vast client is null");
        this.transManager = requireNonNull(transManager, "vast transaction factory is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null").stream()
                .flatMap(sessionPropertiesProvider -> sessionPropertiesProvider.getSessionProperties().stream())
                .collect(toImmutableList());
        LOG.debug("VastSecurityAccessControl#VastSecurityAccessControl: sessionProperties=%s", this.sessionProperties);
    }

    private static boolean isRelevant(final CatalogSchemaTableName table)
    {
        return CATALOG_NAME.equals(table.getCatalogName()) && !INFORMATION_SCHEMA.equals(table.getSchemaTableName().getSchemaName()) ;
    }

    @Override
    public List<ViewExpression> getRowFilters(final SystemSecurityContext context, final CatalogSchemaTableName tableName)
    {
        if (isRelevant(tableName)) {
            try {
                final SchemaTableName schemaTableName = tableName.getSchemaTableName();
                final String schema = schemaTableName.getSchemaName();
                final RowColumnSecurityResponse policy = client.getRowColumnSecurity(transManager.startTransaction(new StartTransactionContext(true, false)),
                        schema, schemaTableName.getTableName());
                LOG.debug("VastSecurityAccessControl#getRowFilters NOT ignoring policy=%s", policy);
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
                LOG.debug("VastSecurityAccessControl#getRowFilters result=%s", result);
                return result;
            } catch (final VastServerException | VastUserException error) {
                LOG.debug(error, "VastSecurityAccessControl#getRowFilters internal failure");
                throw new RuntimeException(error);
            }
        }
        else {
            LOG.debug("getRowFilters ignores request because it's irrelevant: table=%s", tableName);
            return List.of();
        }
    }

    @Override
    public void checkCanShowColumns(final SystemSecurityContext context, final CatalogSchemaTableName table)
    {
        if (isRelevant(table)) {
            try {
                final SchemaTableName schemaTableName = table.getSchemaTableName();
                final String schema = schemaTableName.getSchemaName();
                final RowColumnSecurityResponse policy = client.getRowColumnSecurity(transManager.startTransaction(new StartTransactionContext(true, false)),
                        schema, schemaTableName.getTableName());
                LOG.debug("VastSecurityAccessControl#checkCanShowColumns policy=%s", policy);
                if (policy.getAllowedColumns().isEmpty()) {
                    LOG.debug("VastSecurityAccessControl#checkCanShowColumns no allowed columns so access is denied");
                    AccessDeniedException.denyShowColumns(table.toString());
                } else {
                    LOG.debug("VastSecurityAccessControl#checkCanShowColumns access is allowed");
                }
            } catch (final VastServerException | VastUserException error) {
                LOG.debug(error, "VastSecurityAccessControl#checkCanShowColumns internal failure");
                throw new RuntimeException(error);
            }
        }
        else {
            LOG.debug("checkCanShowColumns ignores request because it's irrelevant: table=%s", table);
        }
    }

    @Override
    public void checkCanSelectFromColumns(final SystemSecurityContext context, final CatalogSchemaTableName table, final Set<String> columns)
    {
        if (isRelevant(table)) {
            try {
                final SchemaTableName schemaTableName = table.getSchemaTableName();
                final String schema = schemaTableName.getSchemaName();
                final RowColumnSecurityResponse policy = client.getRowColumnSecurity(transManager.startTransaction(new StartTransactionContext(true, false)),
                        schema, schemaTableName.getTableName());
                LOG.debug("VastSecurityAccessControl#checkCanSelectFromColumns policy=%s", policy);
                if (policy.getDeniedColumns().stream().anyMatch(columns::contains)) {
                    LOG.debug("VastSecurityAccessControl#checkCanSelectFromColumns requested columns contain at least one denied column: requested columns: %s, denied columns: %s", columns, policy.getDeniedColumns());
                    AccessDeniedException.denyShowColumns(table.toString());
                }
                if (!(columns.stream().allMatch(column -> policy.getAllowedColumns().contains(column) || policy.getMaskedColumns().containsKey(column)))) {
                    LOG.debug("VastSecurityAccessControl#checkCanSelectFromColumns at least one requested columns is not (allowed or masked): requested columns: %s, allowed columns: %s, masked columns: %s", columns, policy.getAllowedColumns(), policy.getMaskedColumns());
                    AccessDeniedException.denyShowColumns(table.toString());
                }
            } catch (final VastServerException | VastUserException error) {
                LOG.debug(error, "VastSecurityAccessControl#checkCanSelectFromColumns internal failure");
                throw new RuntimeException(error);
            }
        }
        else {
            LOG.debug("checkCanSelectFromColumns ignores request because it's irrelevant: table=%s", table);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(final SystemSecurityContext context, final CatalogSchemaTableName table, final Set<String> columns)
    {
        if (isRelevant(table)) {
            try {
                final SchemaTableName schemaTableName = table.getSchemaTableName();
                final String schema = schemaTableName.getSchemaName();
                final RowColumnSecurityResponse policy = client.getRowColumnSecurity(transManager.startTransaction(new StartTransactionContext(true, false)),
                        schema, schemaTableName.getTableName());
                LOG.debug("VastSecurityAccessControl#checkCanCreateViewWithSelectFromColumns policy=%s", policy);
                if (policy.getDeniedColumns().stream().anyMatch(columns::contains)) {
                    LOG.debug("VastSecurityAccessControl#checkCanCreateViewWithSelectFromColumns requested columns contain at least one denied column: requested columns: %s, denied columns: %s", columns, policy.getDeniedColumns());
                    AccessDeniedException.denyShowColumns(table.toString());
                }
                if (!(columns.stream().allMatch(column -> policy.getAllowedColumns().contains(column) || policy.getMaskedColumns().containsKey(column)))) {
                    LOG.debug("VastSecurityAccessControl#checkCanCreateViewWithSelectFromColumns at least one requested columns is not (allowed or masked): requested columns: %s, allowed columns: %s, masked columns: %s", columns, policy.getAllowedColumns(), policy.getMaskedColumns());
                    AccessDeniedException.denyShowColumns(table.toString());
                }
            } catch (final VastServerException | VastUserException error) {
                LOG.debug(error, "VastSecurityAccessControl#checkCanCreateViewWithSelectFromColumns internal failure");
                throw new RuntimeException(error);
            }
        }
        else {
            LOG.debug("checkCanCreateViewWithSelectFromColumns ignores request because it's irrelevant: table=%s", table);
        }
    }

    @Override
    public boolean canAccessCatalog(final SystemSecurityContext context, final String catalogName)
    {
        return true;
    }

    @Override
    public void checkCanSetUser(final Optional<Principal> principal, final String userName)
    {
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner) {
    }

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(Identity identity, Collection<Identity> queryOwners) {
        return SystemAccessControl.super.filterViewQueryOwnedBy(identity, queryOwners);
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner) {
        SystemAccessControl.super.checkCanKillQueryOwnedBy(identity, queryOwner);
    }

    @Override
    public void checkCanImpersonateUser(final Identity identity, final String userName)
    {
    }

    @Override
    public void checkCanExecuteQuery(final Identity identity, final QueryId queryId)
    {
    }

    @Override
    public Set<String> filterCatalogs(final SystemSecurityContext context, final Set<String> catalogs)
    {
        return catalogs.stream().filter(CATALOG_NAME::equals).collect(Collectors.toSet());
    }

    @Override
    public Set<String> filterSchemas(final SystemSecurityContext context, final String catalogName, final Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public Set<SchemaTableName> filterTables(final SystemSecurityContext context, final String catalogName, final Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public Set<String> filterColumns(final SystemSecurityContext context, final CatalogSchemaTableName tableName, final Set<String> columns)
    {
        return columns;
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(final SystemSecurityContext context, final String catalogName, final Map<SchemaTableName, Set<String>> tableColumns)
    {
        return tableColumns;
    }

    @Override
    public boolean canExecuteFunction(final SystemSecurityContext systemSecurityContext, final CatalogSchemaRoutineName functionName)
    {
        return true;
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(final SystemSecurityContext systemSecurityContext, final CatalogSchemaRoutineName functionName)
    {
        return true;
    }

    @Override
    public Set<SchemaFunctionName> filterFunctions(final SystemSecurityContext context, final String catalogName, final Set<SchemaFunctionName> functionNames)
    {
        return functionNames;
    }

    @Override
    public Iterable<EventListener> getEventListeners()
    {
        return ImmutableSet.of();
    }

    @Override
    public Optional<ViewExpression> getColumnMask(final SystemSecurityContext context, final CatalogSchemaTableName tableName, final String columnName, final Type type)
    {
        final ColumnSchema columnSchema = ColumnSchema.builder().setName(columnName).setType(type).build();
        return Optional.ofNullable(getColumnMasks(context, tableName, List.of(columnSchema)).get(columnSchema));
    }

    @Override
    public Map<ColumnSchema, ViewExpression> getColumnMasks(final SystemSecurityContext context, final CatalogSchemaTableName tableName, final List<ColumnSchema> columns)
    {
        if (isRelevant(tableName)) {
            try {
                final SchemaTableName schemaTableName = tableName.getSchemaTableName();
                final String schema = schemaTableName.getSchemaName();
                final RowColumnSecurityResponse policy = client.getRowColumnSecurity(transManager.startTransaction(new StartTransactionContext(true, false)),
                        schema, schemaTableName.getTableName());
                LOG.debug("VastSecurityAccessControl#getColumnMasks policy=%s", policy);
                final Function<String, ColumnSchema> fixKey = columnName ->
                        columns.stream().filter(column -> column.getName().equals(columnName))
                                .findAny().orElseThrow(() -> new IllegalStateException(String.format("Unexpected column: column = %s, columns = %s", columnName, columns)));
                final Function<String, ViewExpression> fixValue = mask -> ViewExpression.builder().expression(mask).build();
                return policy.getMaskedColumns().entrySet().stream()
                        .map(entry -> new AbstractMap.SimpleEntry<>(fixKey.apply(entry.getKey()), fixValue.apply(entry.getValue())))
                        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
            } catch (final VastServerException | VastUserException error) {
                LOG.debug(error, "VastSecurityAccessControl#getColumnMasks  internal failure");
                throw new RuntimeException(error);
            }
        }
        else {
            LOG.debug("getColumnMasks ignores request because it's irrelevant: tableName=%s", tableName);
            return ImmutableMap.of();
        }
    }

    @Override
    public void checkCanCreateSchema(final SystemSecurityContext context, final CatalogSchemaName schema, final Map<String, Object> properties)
    {
    }

    @Override
    public void checkCanInsertIntoTable(final SystemSecurityContext context, final CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(final SystemSecurityContext context, final String catalogName, final String propertyName)
    {
    }

    @Override
    public void checkCanShowSchemas(final SystemSecurityContext context, final String catalogName)
    {
    }

    @Override
    public void checkCanShowTables(final SystemSecurityContext context, final CatalogSchemaName schema)
    {
    }

    @Override
    public void checkCanCreateTable(final SystemSecurityContext context, final CatalogSchemaTableName table, final Map<String, Object> properties)
    {
    }

    @Override
    public void checkCanDeleteFromTable(final SystemSecurityContext context, final CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanUpdateTableColumns(final SystemSecurityContext securityContext, final CatalogSchemaTableName table, final Set<String> updatedColumnNames)
    {
    }

    @Override
    public void checkCanDropSchema(final SystemSecurityContext context, final CatalogSchemaName schema)
    {
    }

    @Override
    public void checkCanRenameSchema(final SystemSecurityContext context, final CatalogSchemaName schema, final String newSchemaName)
    {
    }

    @Override
    public void checkCanDropTable(final SystemSecurityContext context, final CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanRenameColumn(final SystemSecurityContext context, final CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanDropView(final SystemSecurityContext context, final CatalogSchemaTableName view)
    {
    }

    @Override
    public void checkCanAddColumn(final SystemSecurityContext context, final CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanRenameTable(final SystemSecurityContext context, final CatalogSchemaTableName table, final CatalogSchemaTableName newTable)
    {
    }

    @Override
    public void checkCanDropColumn(final SystemSecurityContext context, final CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanCreateView(final SystemSecurityContext context, final CatalogSchemaTableName view)
    {
    }

    @Override
    public void checkCanRenameView(final SystemSecurityContext context, final CatalogSchemaTableName view, final CatalogSchemaTableName newView)
    {
    }

    @Override
    public void checkCanShowCreateTable(final SystemSecurityContext context, final CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanSetSystemSessionProperty(final Identity identity, final String propertyName)
    {
    }

    @Override
    public void checkCanSetSystemSessionProperty(final Identity identity, final QueryId queryId, final String propertyName)
    {
    }

    @Override
    public void checkCanSetTableProperties(final SystemSecurityContext context, final CatalogSchemaTableName table, final Map<String, Optional<Object>> properties)
    {
    }


    /// ============= Stub super implementations with logs =============

    @Override
    public void checkCanReadSystemInformation(final Identity identity)
    {
        SystemAccessControl.super.checkCanReadSystemInformation(identity);
    }

    @Override
    public void checkCanWriteSystemInformation(final Identity identity)
    {
        SystemAccessControl.super.checkCanWriteSystemInformation(identity);
    }

    @Override
    public void checkCanExecuteQuery(final Identity identity)
    {
        SystemAccessControl.super.checkCanExecuteQuery(identity);
    }

    @Override
    public void checkCanCreateCatalog(final SystemSecurityContext context, final String catalog)
    {
        SystemAccessControl.super.checkCanCreateCatalog(context, catalog);
    }

    @Override
    public void checkCanDropCatalog(final SystemSecurityContext context, final String catalog)
    {
        SystemAccessControl.super.checkCanDropCatalog(context, catalog);
    }

    @Override
    public void checkCanSetSchemaAuthorization(final SystemSecurityContext context, final CatalogSchemaName schema, final TrinoPrincipal principal)
    {
        SystemAccessControl.super.checkCanSetSchemaAuthorization(context, schema, principal);
    }

    @Override
    public void checkCanShowCreateSchema(final SystemSecurityContext context, final CatalogSchemaName schemaName)
    {
        SystemAccessControl.super.checkCanShowCreateSchema(context, schemaName);
    }

    @Override
    public void checkCanSetTableComment(final SystemSecurityContext context, final CatalogSchemaTableName table)
    {
        SystemAccessControl.super.checkCanSetTableComment(context, table);
    }

    @Override
    public void checkCanSetViewComment(final SystemSecurityContext context, final CatalogSchemaTableName view)
    {
        SystemAccessControl.super.checkCanSetViewComment(context, view);
    }

    @Override
    public void checkCanSetColumnComment(final SystemSecurityContext context, final CatalogSchemaTableName table)
    {
        SystemAccessControl.super.checkCanSetColumnComment(context, table);
    }

    @Override
    public void checkCanAlterColumn(final SystemSecurityContext context, final CatalogSchemaTableName table)
    {
        SystemAccessControl.super.checkCanAlterColumn(context, table);
    }

    @Override
    public void checkCanSetTableAuthorization(final SystemSecurityContext context, final CatalogSchemaTableName table, final TrinoPrincipal principal)
    {
        SystemAccessControl.super.checkCanSetTableAuthorization(context, table, principal);
    }

    @Override
    public void checkCanTruncateTable(final SystemSecurityContext context, final CatalogSchemaTableName table)
    {
        SystemAccessControl.super.checkCanTruncateTable(context, table);
    }

    @Override
    public void checkCanSetViewAuthorization(final SystemSecurityContext context, final CatalogSchemaTableName view, final TrinoPrincipal principal)
    {
        SystemAccessControl.super.checkCanSetViewAuthorization(context, view, principal);
    }

    @Override
    public void checkCanCreateMaterializedView(final SystemSecurityContext context, final CatalogSchemaTableName materializedView, final Map<String, Object> properties)
    {
        SystemAccessControl.super.checkCanCreateMaterializedView(context, materializedView, properties);
    }

    @Override
    public void checkCanRefreshMaterializedView(final SystemSecurityContext context, final CatalogSchemaTableName materializedView)
    {
        SystemAccessControl.super.checkCanRefreshMaterializedView(context, materializedView);
    }

    @Override
    public void checkCanDropMaterializedView(final SystemSecurityContext context, final CatalogSchemaTableName materializedView)
    {
        SystemAccessControl.super.checkCanDropMaterializedView(context, materializedView);
    }

    @Override
    public void checkCanRenameMaterializedView(final SystemSecurityContext context, final CatalogSchemaTableName view, final CatalogSchemaTableName newView)
    {
        SystemAccessControl.super.checkCanRenameMaterializedView(context, view, newView);
    }

    @Override
    public void checkCanSetMaterializedViewProperties(final SystemSecurityContext context, final CatalogSchemaTableName materializedView, final Map<String, Optional<Object>> properties)
    {
        SystemAccessControl.super.checkCanSetMaterializedViewProperties(context, materializedView, properties);
    }

    @Override
    public void checkCanGrantSchemaPrivilege(final SystemSecurityContext context, final Privilege privilege, final CatalogSchemaName schema, final TrinoPrincipal grantee, final boolean grantOption)
    {
        SystemAccessControl.super.checkCanGrantSchemaPrivilege(context, privilege, schema, grantee, grantOption);
    }

    @Override
    public void checkCanDenySchemaPrivilege(final SystemSecurityContext context, final Privilege privilege, final CatalogSchemaName schema, final TrinoPrincipal grantee)
    {
        SystemAccessControl.super.checkCanDenySchemaPrivilege(context, privilege, schema, grantee);
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(final SystemSecurityContext context, final Privilege privilege, final CatalogSchemaName schema, final TrinoPrincipal revokee, final boolean grantOption)
    {
        SystemAccessControl.super.checkCanRevokeSchemaPrivilege(context, privilege, schema, revokee, grantOption);
    }

    @Override
    public void checkCanGrantTablePrivilege(final SystemSecurityContext context, final Privilege privilege, final CatalogSchemaTableName table, final TrinoPrincipal grantee, final boolean grantOption)
    {
        SystemAccessControl.super.checkCanGrantTablePrivilege(context, privilege, table, grantee, grantOption);
    }

    @Override
    public void checkCanDenyTablePrivilege(final SystemSecurityContext context, final Privilege privilege, final CatalogSchemaTableName table, final TrinoPrincipal grantee)
    {
        SystemAccessControl.super.checkCanDenyTablePrivilege(context, privilege, table, grantee);
    }

    @Override
    public void checkCanRevokeTablePrivilege(final SystemSecurityContext context, final Privilege privilege, final CatalogSchemaTableName table, final TrinoPrincipal revokee, final boolean grantOption)
    {
        SystemAccessControl.super.checkCanRevokeTablePrivilege(context, privilege, table, revokee, grantOption);
    }

    @Override
    public void checkCanGrantEntityPrivilege(final SystemSecurityContext context, final EntityPrivilege privilege, final EntityKindAndName entity, final TrinoPrincipal grantee, final boolean grantOption)
    {
        SystemAccessControl.super.checkCanGrantEntityPrivilege(context, privilege, entity, grantee, grantOption);
    }

    @Override
    public void checkCanDenyEntityPrivilege(final SystemSecurityContext context, final EntityPrivilege privilege, final EntityKindAndName entity, final TrinoPrincipal grantee)
    {
        SystemAccessControl.super.checkCanDenyEntityPrivilege(context, privilege, entity, grantee);
    }

    @Override
    public void checkCanRevokeEntityPrivilege(final SystemSecurityContext context,
                                              final EntityPrivilege privilege,
                                              final EntityKindAndName entity,
                                              final TrinoPrincipal revokee,
                                              final boolean grantOption)
    {
        SystemAccessControl.super.checkCanRevokeEntityPrivilege(context, privilege, entity, revokee, grantOption);
    }

    @Override
    public void checkCanShowRoles(final SystemSecurityContext context)
    {
        SystemAccessControl.super.checkCanShowRoles(context);
    }

    @Override
    public void checkCanCreateRole(final SystemSecurityContext context, final String role, final Optional<TrinoPrincipal> grantor)
    {
        SystemAccessControl.super.checkCanCreateRole(context, role, grantor);
    }

    @Override
    public void checkCanDropRole(final SystemSecurityContext context, final String role)
    {
        SystemAccessControl.super.checkCanDropRole(context, role);
    }

    @Override
    public void checkCanGrantRoles(final SystemSecurityContext context,
                                   final Set<String> roles,
                                   final Set<TrinoPrincipal> grantees,
                                   final boolean adminOption,
                                   final Optional<TrinoPrincipal> grantor)
    {
        SystemAccessControl.super.checkCanGrantRoles(context, roles, grantees, adminOption, grantor);
    }

    @Override
    public void checkCanRevokeRoles(final SystemSecurityContext context,
                                    final Set<String> roles,
                                    final Set<TrinoPrincipal> grantees,
                                    final boolean adminOption,
                                    final Optional<TrinoPrincipal> grantor)
    {
        SystemAccessControl.super.checkCanRevokeRoles(context, roles, grantees, adminOption, grantor);
    }

    @Override
    public void checkCanShowCurrentRoles(final SystemSecurityContext context)
    {
        SystemAccessControl.super.checkCanShowCurrentRoles(context);
    }

    @Override
    public void checkCanShowRoleGrants(final SystemSecurityContext context)
    {
        SystemAccessControl.super.checkCanShowRoleGrants(context);
    }

    @Override
    public void checkCanExecuteProcedure(final SystemSecurityContext systemSecurityContext, final CatalogSchemaRoutineName procedure)
    {
        SystemAccessControl.super.checkCanExecuteProcedure(systemSecurityContext, procedure);
    }

    @Override
    public void checkCanExecuteTableProcedure(final SystemSecurityContext systemSecurityContext, final CatalogSchemaTableName table, final String procedure)
    {
        SystemAccessControl.super.checkCanExecuteTableProcedure(systemSecurityContext, table, procedure);
    }

    @Override
    public void checkCanShowFunctions(final SystemSecurityContext context, final CatalogSchemaName schema)
    {
        SystemAccessControl.super.checkCanShowFunctions(context, schema);
    }

    @Override
    public void checkCanCreateFunction(final SystemSecurityContext systemSecurityContext, final CatalogSchemaRoutineName functionName)
    {
        SystemAccessControl.super.checkCanCreateFunction(systemSecurityContext, functionName);
    }

    @Override
    public void checkCanDropFunction(final SystemSecurityContext systemSecurityContext, final CatalogSchemaRoutineName functionName)
    {
        SystemAccessControl.super.checkCanDropFunction(systemSecurityContext, functionName);
    }

    @Override
    public void checkCanShowCreateFunction(final SystemSecurityContext systemSecurityContext, final CatalogSchemaRoutineName functionName)
    {
        SystemAccessControl.super.checkCanShowCreateFunction(systemSecurityContext, functionName);
    }

    @Override
    public void shutdown()
    {
        SystemAccessControl.super.shutdown();
    }
}
