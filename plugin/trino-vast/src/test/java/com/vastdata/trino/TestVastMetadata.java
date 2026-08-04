/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vastdata.ShapingLoggerFactory;
import com.vastdata.TableLayout;
import com.vastdata.client.QueryDataExtraParams;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastObjectDetails;
import com.vastdata.client.error.VastException;
import com.vastdata.trino.statistics.VastStatisticsManager;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.trino.spi.RefreshType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.vastdata.client.schema.TestVastMetadataUtils.createObjectDetails;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class TestVastMetadata
{
    @Mock VastClient mockClient;
    @Mock ConnectorSession session;

    private AutoCloseable autoCloseable;
    private VastObjectDetails vastObjectDetails;
    private VastPageSourceProvider pageSourceProvider;
    private VastConfig vastConfig;
    private ShapingLoggerFactory shapingLoggerFactory;

    @BeforeEach
    public void setup()
    {
        this.vastObjectDetails = createObjectDetails("table", "id");

        autoCloseable = openMocks(this);
        this.pageSourceProvider = mock(VastPageSourceProvider.class);
        // Mock session properties
        when(session.getProperty(eq("complex_predicate_pushdown"),
                eq(Boolean.class))).thenReturn(false);
        when(session.getProperty(eq("match_substring_pushdown"),
                eq(Boolean.class))).thenReturn(false);
        when(session.getProperty(eq("only_ordered_pushdown"),
                eq(Boolean.class))).thenReturn(false);
        this.vastConfig = new VastConfig();
        this.shapingLoggerFactory = new ShapingLoggerFactory(vastConfig);
    }

    @AfterEach
    public void tearDown()
            throws Exception
    {
        autoCloseable.close();
    }

    private Constraint createConstraint(Map<VastColumnHandle, Domain> domains)
    {
        TupleDomain<ColumnHandle> summary = TupleDomain.withColumnDomains(
                domains
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                Map.Entry::getValue)));
        return new Constraint(summary);
    }

    @Test
    public void testGetTableSchemaNotForImport()
            throws VastException
    {
        Field field1 = new Field("col1",
                FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        Field field2 = new Field("col2",
                FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        List<Field> columnsList = List.of(field1, field2);
        List<VastColumnHandle> columnHandlesList = columnsList.stream().map(
                VastColumnHandle::fromField).collect(Collectors.toList());
        when(mockClient.fetchTableLayout(any(), any(String.class),
                any(String.class), anyInt(), any(QueryDataExtraParams.class), isNull())).thenReturn(
                TableLayout.regularTable(new Schema(columnsList)));
        when(session.getProperty(eq("client_page_size"),
                eq(Integer.class))).thenReturn(5);

        try (RootAllocator allocator = new RootAllocator()) {
            VastMetadata unit = new VastMetadata(pageSourceProvider, mockClient,
                    vastConfig, new VastTransactionHandle(1),
                    mock(VastStatisticsManager.class), new ObjectMapper(),
                    allocator, shapingLoggerFactory);
            VastTableHandle tableHandle = new VastTableHandle("schema", "table",
                    vastObjectDetails, false, false);
            assertNull(tableHandle.getColumnHandlesCache());
            ConnectorTableSchema tableSchema1 = unit.getTableSchema(session,
                    tableHandle);
            assertEquals(tableHandle.getColumnHandlesCache(),
                    columnHandlesList);
            ConnectorTableSchema tableSchema2 = unit.getTableSchema(session,
                    tableHandle);
            assertEquals(tableSchema1.getColumns(), tableSchema2.getColumns());
            assertEquals(tableSchema2.getColumns().size(),
                    columnHandlesList.size());
            ConnectorTableSchema tableSchema3 = unit.getTableSchema(session,
                    tableHandle.forDelete());
            assertEquals(tableSchema1.getColumns(), tableSchema3.getColumns());
            assertEquals(tableSchema3.getColumns().size(),
                    columnHandlesList.size());
            verify(mockClient, times(1)).fetchTableLayout(any(),
                    any(String.class), any(String.class), anyInt(), any(
                    QueryDataExtraParams.class),
                    isNull());
        }
    }

    @Test
    public void testGetTableSchemaForImport()
            throws VastException
    {
        Field field1 = new Field("col1",
                FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        Field field2 = new Field("col2",
                FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        List<Field> columnsList = List.of(field1, field2);
        List<VastColumnHandle> columnHandlesList = columnsList.stream().map(
                VastColumnHandle::fromField).collect(Collectors.toList());
        when(mockClient.fetchTableLayout(any(), any(String.class),
                any(String.class), anyInt(), any(QueryDataExtraParams.class), isNull())).thenReturn(
                TableLayout.regularTable(new Schema(columnsList)));
        when(session.getProperty(eq("client_page_size"),
                eq(Integer.class))).thenReturn(5);

        try (RootAllocator allocator = new RootAllocator()) {
            VastMetadata unit = new VastMetadata(pageSourceProvider, mockClient,
                    vastConfig, new VastTransactionHandle(1),
                    mock(VastStatisticsManager.class), new ObjectMapper(),
                    allocator, shapingLoggerFactory);
            VastTableHandle tableHandle = new VastTableHandle("schema", "table",
                    vastObjectDetails, true, false);
            assertNull(tableHandle.getColumnHandlesCache());
            ConnectorTableSchema tableSchema1 = unit.getTableSchema(session,
                    tableHandle);
            assertEquals(tableHandle
                    .getColumnHandlesCache()
                    .subList(1, columnsList.size() + 1), columnHandlesList);
            ConnectorTableSchema tableSchema2 = unit.getTableSchema(session,
                    tableHandle);
            assertEquals(tableSchema1.getColumns(), tableSchema2.getColumns());
            assertEquals(tableSchema2.getColumns().size(),
                    columnHandlesList.size() + 1);
            verify(mockClient, times(1)).fetchTableLayout(any(),
                    any(String.class), any(String.class), anyInt(), any(QueryDataExtraParams.class),
                    isNull());
        }
    }

    @Test
    public void testApplyFilter_ScalarColumnsOnly()
    {
        // Test that only scalar columns (no children) are supported for predicate pushdown
        VastColumnHandle stringColumn = VastColumnHandle.fromField(
                Field.nullable("string_col", ArrowType.Utf8.INSTANCE));
        Field arrayElementField = Field.nullable("element",
                ArrowType.Utf8.INSTANCE);
        Field arrayField = new Field("array_col",
                FieldType.nullable(new ArrowType.List()),
                List.of(arrayElementField));
        VastColumnHandle arrayColumn = VastColumnHandle.fromField(arrayField);

        Map<VastColumnHandle, Domain> domains = new HashMap<>();
        domains.put(stringColumn, singleValue(VARCHAR, utf8Slice("test")));
        domains.put(arrayColumn, singleValue(VARCHAR,
                utf8Slice("test"))); // This should be filtered out

        Constraint constraint = createConstraint(domains);

        VastTableHandle tableHandle = new VastTableHandle("schema", "table",
                vastObjectDetails, false, false);
        try (RootAllocator allocator = new RootAllocator()) {
            VastMetadata metadata = new VastMetadata(pageSourceProvider,
                    mockClient, vastConfig, new VastTransactionHandle(1),
                    mock(VastStatisticsManager.class), new ObjectMapper(),
                    allocator, shapingLoggerFactory);

            Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(
                    session, tableHandle, constraint);

            assertTrue(result.isPresent());
            ConstraintApplicationResult<ConnectorTableHandle> applicationResult = result.orElseThrow();

            // Should only have scalar columns in enforced predicate
            VastTableHandle newTableHandle = (VastTableHandle) applicationResult.getHandle();
            TupleDomain<VastColumnHandle> enforcedPredicate = newTableHandle.getPredicate();
            assertTrue(enforcedPredicate.getDomains().isPresent());

            Set<VastColumnHandle> enforcedColumns = enforcedPredicate
                    .getDomains()
                    .orElseThrow()
                    .keySet();
            assertTrue(enforcedColumns.contains(stringColumn));
            assertFalse(enforcedColumns.contains(
                    arrayColumn)); // Array column should be filtered out
        }
    }

    @Test
    public void testApplyFilter_OnlyOrderedPushdownFalse()
    {
        // Test with only_ordered_pushdown=false (default behavior) - should work with scalar columns
        VastColumnHandle stringColumn = VastColumnHandle.fromField(
                Field.nullable("string_col", ArrowType.Utf8.INSTANCE));

        Map<VastColumnHandle, Domain> domains = new HashMap<>();
        domains.put(stringColumn, singleValue(VARCHAR, utf8Slice("test")));

        Constraint constraint = createConstraint(domains);

        VastTableHandle tableHandle = new VastTableHandle("schema", "table",
                vastObjectDetails, false, false);
        try (RootAllocator allocator = new RootAllocator()) {
            VastMetadata metadata = new VastMetadata(pageSourceProvider,
                    mockClient, vastConfig, new VastTransactionHandle(1),
                    mock(VastStatisticsManager.class), new ObjectMapper(),
                    allocator, shapingLoggerFactory);

            Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(
                    session, tableHandle, constraint);

            // Should succeed because only_ordered_pushdown=false allows scalar columns regardless of sorting
            assertTrue(result.isPresent());
            ConstraintApplicationResult<ConnectorTableHandle> applicationResult = result.orElseThrow();

            VastTableHandle newTableHandle = (VastTableHandle) applicationResult.getHandle();
            TupleDomain<VastColumnHandle> enforcedPredicate = newTableHandle.getPredicate();
            assertTrue(enforcedPredicate.getDomains().isPresent());

            Set<VastColumnHandle> enforcedColumns = enforcedPredicate
                    .getDomains()
                    .orElseThrow()
                    .keySet();
            assertTrue(enforcedColumns.contains(stringColumn));
        }
    }

    @Test
    public void testApplyFilter_OnlyOrderedPushdownTrue()
    {
        // Test with only_ordered_pushdown=true - should fail because column is not sorted
        VastColumnHandle stringColumn = VastColumnHandle.fromField(
                Field.nullable("string_col", ArrowType.Utf8.INSTANCE));

        Map<VastColumnHandle, Domain> domains = new HashMap<>();
        domains.put(stringColumn, singleValue(VARCHAR, utf8Slice("test")));

        Constraint constraint = createConstraint(domains);

        VastTableHandle tableHandle = new VastTableHandle("schema", "table",
                vastObjectDetails, false, false);
        try (RootAllocator allocator = new RootAllocator()) {
            VastMetadata metadata = new VastMetadata(pageSourceProvider,
                    mockClient, vastConfig, new VastTransactionHandle(1),
                    mock(VastStatisticsManager.class), new ObjectMapper(),
                    allocator, shapingLoggerFactory);

            // Mock session with only_ordered_pushdown=true
            when(session.getProperty(eq("only_ordered_pushdown"),
                    eq(Boolean.class))).thenReturn(true);

            Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(
                    session, tableHandle, constraint);

            // Should return empty because no columns can be pushed down (column is not sorted)
            assertFalse(result.isPresent());
        }
    }

    @Test
    public void testApplyFilter_OnlyOrderedPushdownWithMixedColumns()
    {
        // Test with only_ordered_pushdown=true - table has 4 columns: 2 sorted, 2 non-sorted
        // Predicates on both sorted and non-sorted columns - only sorted should be pushed down

        // Create 4 columns: 2 sorted, 2 non-sorted
        VastColumnHandle sortedColumn1 = VastColumnHandle.fromField(
                Field.nullable("sorted_col1", ArrowType.Utf8.INSTANCE));
        VastColumnHandle sortedColumn2 = VastColumnHandle.fromField(
                Field.nullable("sorted_col2", new ArrowType.Int(32, true)));
        VastColumnHandle nonSortedColumn1 = VastColumnHandle.fromField(
                Field.nullable("non_sorted_col1", ArrowType.Utf8.INSTANCE));
        VastColumnHandle nonSortedColumn2 = VastColumnHandle.fromField(
                Field.nullable("non_sorted_col2", new ArrowType.Int(32, true)));

        // Create predicates on all 4 columns
        Map<VastColumnHandle, Domain> domains = new HashMap<>();
        domains.put(sortedColumn1,
                singleValue(VARCHAR, utf8Slice("sorted_value1")));
        domains.put(sortedColumn2,
                singleValue(io.trino.spi.type.IntegerType.INTEGER, 100L));
        domains.put(nonSortedColumn1,
                singleValue(VARCHAR, utf8Slice("non_sorted_value1")));
        domains.put(nonSortedColumn2,
                singleValue(io.trino.spi.type.IntegerType.INTEGER, 200L));

        Constraint constraint = createConstraint(domains);

        // Create table handle with sorted columns
        VastTableHandle tableHandle = new VastTableHandle("schema", "table",
                vastObjectDetails, false, false);
        tableHandle = tableHandle.withSortedColumns(
                List.of("sorted_col1", "sorted_col2"));
        try (RootAllocator allocator = new RootAllocator()) {
            VastMetadata metadata = new VastMetadata(pageSourceProvider,
                    mockClient, vastConfig, new VastTransactionHandle(1),
                    mock(VastStatisticsManager.class), new ObjectMapper(),
                    allocator, shapingLoggerFactory);

            // Mock session with only_ordered_pushdown=true
            when(session.getProperty(eq("only_ordered_pushdown"),
                    eq(Boolean.class))).thenReturn(true);

            Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(
                    session, tableHandle, constraint);

            // Should succeed and only have sorted columns in enforced predicate
            assertTrue(result.isPresent());
            ConstraintApplicationResult<ConnectorTableHandle> applicationResult = result.orElseThrow();

            VastTableHandle newTableHandle = (VastTableHandle) applicationResult.getHandle();
            TupleDomain<VastColumnHandle> enforcedPredicate = newTableHandle.getPredicate();
            assertTrue(enforcedPredicate.getDomains().isPresent());

            Set<VastColumnHandle> enforcedColumns = enforcedPredicate
                    .getDomains()
                    .orElseThrow()
                    .keySet();

            // Only sorted columns should be in enforced predicate
            assertTrue(enforcedColumns.contains(sortedColumn1));
            assertTrue(enforcedColumns.contains(sortedColumn2));
            assertFalse(enforcedColumns.contains(nonSortedColumn1));
            assertFalse(enforcedColumns.contains(nonSortedColumn2));
        }
    }

    @Test
    public void testApplyFilter_NoChangesNeeded()
    {
        // Test when no changes are needed (same predicate as existing table) - using integer type
        VastColumnHandle intColumn = VastColumnHandle.fromField(
                Field.nullable("int_col", new ArrowType.Int(32, true)));

        Map<VastColumnHandle, Domain> domains = new HashMap<>();
        domains.put(intColumn,
                singleValue(io.trino.spi.type.IntegerType.INTEGER, 42L));

        Constraint constraint = createConstraint(domains);

        // Create table handle with the same predicate
        TupleDomain<VastColumnHandle> existingPredicate = TupleDomain.withColumnDomains(
                domains);
        VastTableHandle tableHandle = new VastTableHandle("schema", "table",
                vastObjectDetails, false, false).withPredicate(existingPredicate,
                Optional.empty(), List.of());

        try (RootAllocator allocator = new RootAllocator()) {
            VastMetadata metadata = new VastMetadata(pageSourceProvider,
                    mockClient, vastConfig, new VastTransactionHandle(1),
                    mock(VastStatisticsManager.class), new ObjectMapper(),
                    allocator, shapingLoggerFactory);

            Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(
                    session, tableHandle, constraint);

            // Should return empty when no changes are needed
            assertFalse(result.isPresent());
        }
    }

    @Test
    public void testApplyFilter_EmptyConstraint()
    {
        // Test with empty constraint
        TupleDomain<ColumnHandle> emptySummary = TupleDomain.all();
        Constraint constraint = new Constraint(emptySummary);

        VastTableHandle tableHandle = new VastTableHandle("schema", "table",
                vastObjectDetails, false, false);
        try (RootAllocator allocator = new RootAllocator()) {
            VastMetadata metadata = new VastMetadata(pageSourceProvider,
                    mockClient, vastConfig, new VastTransactionHandle(1),
                    mock(VastStatisticsManager.class), new ObjectMapper(),
                    allocator, shapingLoggerFactory);

            Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(
                    session, tableHandle, constraint);

            // Should return empty when no constraints to apply
            assertFalse(result.isPresent());
        }
    }

    @Test
    public void testApplyFilter_WithExistingPredicate()
    {
        // Test applying new predicate on table with existing predicate
        VastColumnHandle stringColumn = VastColumnHandle.fromField(
                Field.nullable("string_col", ArrowType.Utf8.INSTANCE));

        // Existing predicate
        Map<VastColumnHandle, Domain> existingDomains = new HashMap<>();
        existingDomains.put(stringColumn,
                singleValue(VARCHAR, utf8Slice("existing")));
        TupleDomain<VastColumnHandle> existingPredicate = TupleDomain.withColumnDomains(
                existingDomains);

        // New constraint - use a range that intersects with existing
        Map<VastColumnHandle, Domain> newDomains = new HashMap<>();
        newDomains.put(stringColumn,
                singleValue(VARCHAR, utf8Slice("existing")));
        Constraint constraint = createConstraint(newDomains);

        VastTableHandle tableHandle = new VastTableHandle("schema", "table",
                vastObjectDetails, false, false).withPredicate(existingPredicate,
                Optional.empty(), List.of());

        try (RootAllocator allocator = new RootAllocator()) {
            VastMetadata metadata = new VastMetadata(pageSourceProvider,
                    mockClient, vastConfig, new VastTransactionHandle(1),
                    mock(VastStatisticsManager.class), new ObjectMapper(),
                    allocator, shapingLoggerFactory);

            Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(
                    session, tableHandle, constraint);

            // Should return empty when predicates are the same
            assertFalse(result.isPresent());
        }
    }

    @Test
    public void testApplyFilter_NestedStructScalarFields()
    {
        // Test predicate on leaf fields (a and b) within a nested struct column
        // Create a struct with leaf fields a and b
        Field fieldA = Field.nullable("a", ArrowType.Utf8.INSTANCE);
        Field fieldB = Field.nullable("b", new ArrowType.Int(32, true));
        // Create column handles for the leaf fields within the struct
        VastColumnHandle leafColumnA = VastColumnHandle.fromField(fieldA);
        VastColumnHandle leafColumnB = VastColumnHandle.fromField(fieldB);

        Map<VastColumnHandle, Domain> domains = new HashMap<>();
        domains.put(leafColumnA, singleValue(VARCHAR, utf8Slice("test_a")));
        domains.put(leafColumnB,
                singleValue(io.trino.spi.type.IntegerType.INTEGER, 42L));

        Constraint constraint = createConstraint(domains);

        VastTableHandle tableHandle = new VastTableHandle("schema", "table",
                vastObjectDetails, false, false);
        try (RootAllocator allocator = new RootAllocator()) {
            VastMetadata metadata = new VastMetadata(pageSourceProvider,
                    mockClient, vastConfig, new VastTransactionHandle(1),
                    mock(VastStatisticsManager.class), new ObjectMapper(),
                    allocator, shapingLoggerFactory);

            Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(
                    session, tableHandle, constraint);

            // Should succeed because leaf fields (a and b) within nested structs are supported for predicate pushdown
            assertTrue(result.isPresent());
            ConstraintApplicationResult<ConnectorTableHandle> applicationResult = result.orElseThrow();

            // Should have both leaf columns in enforced predicate
            VastTableHandle newTableHandle = (VastTableHandle) applicationResult.getHandle();
            TupleDomain<VastColumnHandle> enforcedPredicate = newTableHandle.getPredicate();
            assertTrue(enforcedPredicate.getDomains().isPresent());

            Set<VastColumnHandle> enforcedColumns = enforcedPredicate
                    .getDomains()
                    .orElseThrow()
                    .keySet();
            assertTrue(enforcedColumns.contains(leafColumnA));
            assertTrue(enforcedColumns.contains(leafColumnB));
        }
    }

    @Test
    public void testApplyFilter_StringPrefixPredicate()
    {
        // Test string prefix predicate (LIKE 'prefix%') using range domain
        VastColumnHandle stringColumn = VastColumnHandle.fromField(
                Field.nullable("string_col", ArrowType.Utf8.INSTANCE));

        // Create a range domain that represents "prefix%" (>= 'prefix' AND < 'prefiy')
        Domain prefixDomain = Domain.create(
                io.trino.spi.predicate.ValueSet.ofRanges(
                        io.trino.spi.predicate.Range.range(VARCHAR,
                                utf8Slice("prefix"), true, utf8Slice("prefiy"),
                                false)), false);
        Map<VastColumnHandle, Domain> domains = new HashMap<>();
        domains.put(stringColumn, prefixDomain);

        Constraint constraint = createConstraint(domains);

        VastTableHandle tableHandle = new VastTableHandle("schema", "table",
                vastObjectDetails, false, false);
        try (RootAllocator allocator = new RootAllocator()) {
            VastMetadata metadata = new VastMetadata(pageSourceProvider,
                    mockClient, vastConfig, new VastTransactionHandle(1),
                    mock(VastStatisticsManager.class), new ObjectMapper(),
                    allocator, shapingLoggerFactory);

            Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(
                    session, tableHandle, constraint);

            assertTrue(result.isPresent());
            ConstraintApplicationResult<ConnectorTableHandle> applicationResult = result.orElseThrow();

            // Should have string column in enforced predicate
            VastTableHandle newTableHandle = (VastTableHandle) applicationResult.getHandle();
            TupleDomain<VastColumnHandle> enforcedPredicate = newTableHandle.getPredicate();
            assertTrue(enforcedPredicate.getDomains().isPresent());

            Set<VastColumnHandle> enforcedColumns = enforcedPredicate
                    .getDomains()
                    .orElseThrow()
                    .keySet();
            assertTrue(enforcedColumns.contains(stringColumn));
        }
    }

    @Test
    public void testEverythingImplemented()
            throws NoSuchMethodException
    {
        assertAllMethodsOverridden(ConnectorMetadata.class, VastMetadata.class,
                Set.of(ConnectorMetadata.class.getMethod("beginQuery",
                                ConnectorSession.class),
                        ConnectorMetadata.class.getMethod("cleanupQuery",
                                ConnectorSession.class),
                        ConnectorMetadata.class.getMethod("listRoles",
                                ConnectorSession.class),
                        ConnectorMetadata.class.getMethod("grantRoles",
                                ConnectorSession.class, Set.class, Set.class,
                                boolean.class, Optional.class),
                        ConnectorMetadata.class.getMethod("revokeRoles",
                                ConnectorSession.class, Set.class, Set.class,
                                boolean.class, Optional.class),
                        ConnectorMetadata.class.getMethod("roleExists",
                                ConnectorSession.class, String.class),
                        ConnectorMetadata.class.getMethod("dropRole",
                                ConnectorSession.class, String.class),
                        ConnectorMetadata.class.getMethod("createRole",
                                ConnectorSession.class, String.class,
                                Optional.class),
                        ConnectorMetadata.class.getMethod("listRoleGrants",
                                ConnectorSession.class, TrinoPrincipal.class),
                        ConnectorMetadata.class.getMethod("listEnabledRoles",
                                ConnectorSession.class),
                        ConnectorMetadata.class.getMethod(
                                "grantTablePrivileges", ConnectorSession.class,
                                SchemaTableName.class, Set.class,
                                TrinoPrincipal.class, boolean.class),
                        ConnectorMetadata.class.getMethod(
                                "grantSchemaPrivileges", ConnectorSession.class,
                                String.class, Set.class, TrinoPrincipal.class,
                                boolean.class),
                        ConnectorMetadata.class.getMethod(
                                "revokeTablePrivileges", ConnectorSession.class,
                                SchemaTableName.class, Set.class,
                                TrinoPrincipal.class, boolean.class),
                        ConnectorMetadata.class.getMethod(
                                "revokeSchemaPrivileges",
                                ConnectorSession.class, String.class, Set.class,
                                TrinoPrincipal.class, boolean.class),
                        ConnectorMetadata.class.getMethod("denyTablePrivileges",
                                ConnectorSession.class, SchemaTableName.class,
                                Set.class, TrinoPrincipal.class),
                        ConnectorMetadata.class.getMethod(
                                "denySchemaPrivileges", ConnectorSession.class,
                                String.class, Set.class, TrinoPrincipal.class),
                        ConnectorMetadata.class.getMethod("listApplicableRoles",
                                ConnectorSession.class, TrinoPrincipal.class),
                        ConnectorMetadata.class.getMethod("listTablePrivileges",
                                ConnectorSession.class,
                                SchemaTablePrefix.class),
                        ConnectorMetadata.class.getMethod("getSchemaOwner",
                                ConnectorSession.class, String.class),
                        ConnectorMetadata.class.getMethod("getSchemaProperties",
                                ConnectorSession.class, String.class),
                        ConnectorMetadata.class.getMethod("getViewProperties",
                                ConnectorSession.class, SchemaTableName.class),
                        ConnectorMetadata.class.getMethod(
                                "setMaterializedViewProperties",
                                ConnectorSession.class, SchemaTableName.class,
                                Map.class), ConnectorMetadata.class.getMethod(
                                "renameMaterializedView",
                                ConnectorSession.class, SchemaTableName.class,
                                SchemaTableName.class),
                        ConnectorMetadata.class.getMethod(
                                "getMaterializedViewProperties",
                                ConnectorSession.class, SchemaTableName.class,
                                ConnectorMaterializedViewDefinition.class),
                        ConnectorMetadata.class.getMethod(
                                "getMaterializedViewFreshness",
                                ConnectorSession.class, SchemaTableName.class),
                        ConnectorMetadata.class.getMethod("getMaxWriterTasks",
                                ConnectorSession.class),
                        ConnectorMetadata.class.getMethod("getMaterializedView",
                                ConnectorSession.class, SchemaTableName.class),
                        ConnectorMetadata.class.getMethod(
                                "getMaterializedViews", ConnectorSession.class,
                                Optional.class),
                        ConnectorMetadata.class.getMethod(
                                "getNewTableWriterScalingOptions",
                                ConnectorSession.class, SchemaTableName.class,
                                Map.class),
                        ConnectorMetadata.class.getMethod("isView",
                                ConnectorSession.class, SchemaTableName.class),
                        ConnectorMetadata.class.getMethod("getViews",
                                ConnectorSession.class, Optional.class),
                        ConnectorMetadata.class.getMethod(
                                "listMaterializedViews", ConnectorSession.class,
                                Optional.class),
                        ConnectorMetadata.class.getMethod("getRelationTypes",
                                ConnectorSession.class, Optional.class),
                        ConnectorMetadata.class.getMethod(
                                "createMaterializedView",
                                ConnectorSession.class, SchemaTableName.class,
                                ConnectorMaterializedViewDefinition.class,
                                Map.class, boolean.class, boolean.class),
                        ConnectorMetadata.class.getMethod("addField",
                                ConnectorSession.class,
                                ConnectorTableHandle.class, List.class,
                                String.class, Type.class, boolean.class),
                        ConnectorMetadata.class.getMethod("renameField",
                                ConnectorSession.class,
                                ConnectorTableHandle.class, List.class,
                                String.class),
                        ConnectorMetadata.class.getMethod("dropField",
                                ConnectorSession.class,
                                ConnectorTableHandle.class, ColumnHandle.class,
                                List.class), ConnectorMetadata.class.getMethod(
                                "dropMaterializedView", ConnectorSession.class,
                                SchemaTableName.class),
                        ConnectorMetadata.class.getMethod(
                                "dropNotNullConstraint", ConnectorSession.class,
                                ConnectorTableHandle.class, ColumnHandle.class),
                        ConnectorMetadata.class.getMethod("setTableComment",
                                ConnectorSession.class,
                                ConnectorTableHandle.class, Optional.class),
                        ConnectorMetadata.class.getMethod("setViewComment",
                                ConnectorSession.class, SchemaTableName.class,
                                Optional.class),
                        ConnectorMetadata.class.getMethod(
                                "refreshMaterializedView",
                                ConnectorSession.class, SchemaTableName.class),
                        ConnectorMetadata.class.getMethod(
                                "beginRefreshMaterializedView",
                                ConnectorSession.class,
                                ConnectorTableHandle.class, List.class,
                                boolean.class, RetryMode.class,
                                RefreshType.class),
                        ConnectorMetadata.class.getMethod(
                                "finishRefreshMaterializedView",
                                ConnectorSession.class,
                                ConnectorTableHandle.class,
                                ConnectorInsertTableHandle.class,
                                Collection.class, Collection.class, List.class,
                                boolean.class, boolean.class),
                        ConnectorMetadata.class.getMethod(
                                "setMaterializedViewColumnComment",
                                ConnectorSession.class, SchemaTableName.class,
                                String.class, Optional.class),
                        ConnectorMetadata.class.getMethod("setColumnComment",
                                ConnectorSession.class,
                                ConnectorTableHandle.class, ColumnHandle.class,
                                Optional.class),
                        ConnectorMetadata.class.getMethod(
                                "setViewAuthorization", ConnectorSession.class,
                                SchemaTableName.class, TrinoPrincipal.class),
                        ConnectorMetadata.class.getMethod(
                                "setTableAuthorization", ConnectorSession.class,
                                SchemaTableName.class, TrinoPrincipal.class),
                        ConnectorMetadata.class.getMethod(
                                "setViewColumnComment", ConnectorSession.class,
                                SchemaTableName.class, String.class,
                                Optional.class),
                        ConnectorMetadata.class.getMethod(
                                "setSchemaAuthorization",
                                ConnectorSession.class, String.class,
                                TrinoPrincipal.class),
                        ConnectorMetadata.class.getMethod("getInfo",
                                ConnectorSession.class,
                                ConnectorTableHandle.class),
                        ConnectorMetadata.class.getMethod(
                                "getStatisticsCollectionMetadataForWrite",
                                ConnectorSession.class,
                                ConnectorTableMetadata.class, boolean.class),
                        ConnectorMetadata.class.getMethod(
                                "getStatisticsCollectionMetadataForWrite",
                                ConnectorSession.class,
                                ConnectorTableMetadata.class),
                        ConnectorMetadata.class.getMethod(
                                "getInsertWriterScalingOptions",
                                ConnectorSession.class,
                                ConnectorTableHandle.class),
                        ConnectorMetadata.class.getMethod("getTableName",
                                ConnectorSession.class,
                                ConnectorTableHandle.class),
                        ConnectorMetadata.class.getMethod("getSupportedType",
                                ConnectorSession.class, Map.class, Type.class),
                        ConnectorMetadata.class.getMethod("setColumnType",
                                ConnectorSession.class,
                                ConnectorTableHandle.class, ColumnHandle.class,
                                Type.class),
                        ConnectorMetadata.class.getMethod("setFieldType",
                                ConnectorSession.class,
                                ConnectorTableHandle.class, List.class,
                                Type.class),
                        ConnectorMetadata.class.getMethod("redirectTable",
                                ConnectorSession.class, SchemaTableName.class),
                        ConnectorMetadata.class.getMethod("getSystemTable",
                                ConnectorSession.class, SchemaTableName.class),
                        ConnectorMetadata.class.getMethod(
                                "getTableHandleForExecute",
                                ConnectorSession.class,
                                ConnectorAccessControl.class,
                                ConnectorTableHandle.class, String.class,
                                Map.class, RetryMode.class),
                        ConnectorMetadata.class.getMethod(
                                "getLayoutForTableExecute",
                                ConnectorSession.class,
                                ConnectorTableExecuteHandle.class),
                        ConnectorMetadata.class.getMethod("beginTableExecute",
                                ConnectorSession.class,
                                ConnectorTableExecuteHandle.class,
                                ConnectorTableHandle.class),
                        ConnectorMetadata.class.getMethod("finishTableExecute",
                                ConnectorSession.class,
                                ConnectorTableExecuteHandle.class,
                                Collection.class, List.class),
                        ConnectorMetadata.class.getMethod("executeTableExecute",
                                ConnectorSession.class,
                                ConnectorTableExecuteHandle.class),
                        ConnectorMetadata.class.getMethod("truncateTable",
                                ConnectorSession.class,
                                ConnectorTableHandle.class),
                        ConnectorMetadata.class.getMethod(
                                "allowSplittingReadIntoMultipleSubQueries",
                                ConnectorSession.class,
                                ConnectorTableHandle.class),
                        ConnectorMetadata.class.getMethod(
                                "delegateMaterializedViewRefreshToConnector",
                                ConnectorSession.class, SchemaTableName.class),
                        ConnectorMetadata.class.getMethod(
                                "getLanguageFunctions", ConnectorSession.class,
                                SchemaFunctionName.class),
                        ConnectorMetadata.class.getMethod("listFunctions",
                                ConnectorSession.class, String.class),
                        ConnectorMetadata.class.getMethod(
                                "createLanguageFunction",
                                ConnectorSession.class,
                                SchemaFunctionName.class,
                                LanguageFunction.class, boolean.class),
                        ConnectorMetadata.class.getMethod(
                                "dropLanguageFunction", ConnectorSession.class,
                                SchemaFunctionName.class, String.class),
                        ConnectorMetadata.class.getMethod("getFunctionMetadata",
                                ConnectorSession.class, FunctionId.class),
                        ConnectorMetadata.class.getMethod(
                                "getAggregationFunctionMetadata",
                                ConnectorSession.class, FunctionId.class),
                        ConnectorMetadata.class.getMethod(
                                "getFunctionDependencies",
                                ConnectorSession.class, FunctionId.class,
                                BoundSignature.class),
                        ConnectorMetadata.class.getMethod(
                                "listLanguageFunctions", ConnectorSession.class,
                                String.class),
                        ConnectorMetadata.class.getMethod(
                                "languageFunctionExists",
                                ConnectorSession.class,
                                SchemaFunctionName.class, String.class),
                        ConnectorMetadata.class.getMethod("getFunctions",
                                ConnectorSession.class,
                                SchemaFunctionName.class),
                        ConnectorMetadata.class.getMethod("applyTableFunction",
                                ConnectorSession.class,
                                ConnectorTableFunctionHandle.class),
                        ConnectorMetadata.class.getMethod("applyUpdate",
                                ConnectorSession.class,
                                ConnectorTableHandle.class, Map.class),
                        ConnectorMetadata.class.getMethod("executeUpdate",
                                ConnectorSession.class,
                                ConnectorTableHandle.class),
                        ConnectorMetadata.class.getMethod("applyJoin",
                                ConnectorSession.class, JoinType.class,
                                ConnectorTableHandle.class,
                                ConnectorTableHandle.class, List.class,
                                Map.class, Map.class, JoinStatistics.class),
                        ConnectorMetadata.class.getMethod("applyJoin",
                                ConnectorSession.class, JoinType.class,
                                ConnectorTableHandle.class,
                                ConnectorTableHandle.class,
                                ConnectorExpression.class, Map.class, Map.class,
                                JoinStatistics.class),
                        ConnectorMetadata.class.getMethod("resolveIndex",
                                ConnectorSession.class,
                                ConnectorTableHandle.class, Set.class,
                                Set.class, TupleDomain.class),
                        ConnectorMetadata.class.getMethod("applyTopN",
                                ConnectorSession.class,
                                ConnectorTableHandle.class, long.class,
                                List.class, Map.class),
                        ConnectorMetadata.class.getMethod("applySample",
                                ConnectorSession.class,
                                ConnectorTableHandle.class, SampleType.class,
                                double.class),
                        ConnectorMetadata.class.getMethod("applyAggregation",
                                ConnectorSession.class,
                                ConnectorTableHandle.class, List.class,
                                Map.class, List.class),
                        ConnectorMetadata.class.getMethod(
                                "getCommonPartitioningHandle",
                                ConnectorSession.class,
                                ConnectorPartitioningHandle.class,
                                ConnectorPartitioningHandle.class),
                        ConnectorMetadata.class.getMethod(
                                "applyTableScanRedirect",
                                ConnectorSession.class,
                                ConnectorTableHandle.class),
                        ConnectorMetadata.class.getMethod("validateScan",
                                ConnectorSession.class,
                                ConnectorTableHandle.class),
                        ConnectorMetadata.class.getMethod(
                                "streamRelationComments",
                                ConnectorSession.class, Optional.class,
                                UnaryOperator.class),
                        ConnectorMetadata.class.getMethod(
                                "grantTableBranchPrivileges",
                                ConnectorSession.class, SchemaTableName.class,
                                String.class, Set.class, TrinoPrincipal.class,
                                boolean.class),
                        ConnectorMetadata.class.getMethod(
                                "revokeTableBranchPrivileges",
                                ConnectorSession.class, SchemaTableName.class,
                                String.class, Set.class, TrinoPrincipal.class,
                                boolean.class),
                        ConnectorMetadata.class.getMethod(
                                "denyTableBranchPrivileges",
                                ConnectorSession.class, SchemaTableName.class,
                                String.class, Set.class, TrinoPrincipal.class),
                        ConnectorMetadata.class.getMethod("listBranches",
                                ConnectorSession.class, SchemaTableName.class),
                        ConnectorMetadata.class.getMethod("createBranch",
                                ConnectorSession.class,
                                ConnectorTableHandle.class, String.class,
                                Optional.class, SaveMode.class, Map.class),
                        ConnectorMetadata.class.getMethod("dropBranch",
                                ConnectorSession.class,
                                ConnectorTableHandle.class, String.class),
                        ConnectorMetadata.class.getMethod("branchExists",
                                ConnectorSession.class, SchemaTableName.class,
                                String.class),
                        ConnectorMetadata.class.getMethod("fastForwardBranch",
                                ConnectorSession.class,
                                ConnectorTableHandle.class, String.class,
                                String.class),
                        ConnectorMetadata.class.getMethod("refreshView",
                                ConnectorSession.class, SchemaTableName.class,
                                ConnectorViewDefinition.class),
                        ConnectorMetadata.class.getMethod(
                                "setMaterializedViewAuthorization",
                                ConnectorSession.class, SchemaTableName.class,
                                TrinoPrincipal.class),
                        ConnectorMetadata.class.getMethod("listTableColumns",
                                ConnectorSession.class,
                                SchemaTablePrefix.class),
                        ConnectorMetadata.class.getMethod("streamTableColumns",
                                ConnectorSession.class,
                                SchemaTablePrefix.class)));
    }

    public static <I, C extends I> void assertAllMethodsOverridden(
            Class<I> iface, Class<C> clazz, Set<Method> exclusions)
    {
        checkArgument(iface.isAssignableFrom(clazz),
                "%s is not supertype of %s", iface, clazz);
        exclusions = new HashSet<>(exclusions);
        for (Method method : iface.getMethods()) {
            if (Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            if (method.getDeclaringClass() == Object.class) {
                continue;
            }
            try {
                Method override = clazz.getDeclaredMethod(method.getName(),
                        method.getParameterTypes());
                if (!method.getReturnType().isAssignableFrom(
                        override.getReturnType())) {
                    fail(format("%s is not assignable from %s for method %s",
                            method.getReturnType(), override.getReturnType(),
                            method));
                }
            }
            catch (NoSuchMethodException e) {
                if (!exclusions.remove(method)) {
                    fail(format("%s does not override [%s]", clazz.getName(),
                            method));
                }
            }
        }

        if (!exclusions.isEmpty()) {
            fail("Following exclusions are redundant: " + exclusions);
        }
    }
}
