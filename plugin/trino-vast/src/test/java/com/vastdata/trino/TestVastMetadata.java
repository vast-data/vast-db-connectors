/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class TestVastMetadata
{
    @Mock VastClient mockClient;
    @Mock ConnectorSession session;

    private AutoCloseable autoCloseable;

    @BeforeEach
    public void setup()
    {
        autoCloseable = openMocks(this);
        // Mock session properties
        when(session.getProperty(eq("complex_predicate_pushdown"), eq(Boolean.class))).thenReturn(false);
        when(session.getProperty(eq("match_substring_pushdown"), eq(Boolean.class))).thenReturn(false);
        when(session.getProperty(eq("only_ordered_pushdown"), eq(Boolean.class))).thenReturn(false);
    }

    @AfterEach
    public void tearDown()
            throws Exception
    {
        autoCloseable.close();
    }

    private Constraint createConstraint(Map<VastColumnHandle, Domain> domains)
    {
        TupleDomain<ColumnHandle> summary = TupleDomain.withColumnDomains(domains.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> (ColumnHandle) entry.getKey(),
                        Map.Entry::getValue)));
        return new Constraint(summary);
    }

    @Test
    public void testGetTableSchemaNotForImport()
            throws VastException
    {
        Field field1 = new Field("col1", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        Field field2 = new Field("col2", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        List<Field> columnsList = List.of(field1, field2);
        List<VastColumnHandle> columnHandlesList = columnsList.stream().map(VastColumnHandle::fromField).collect(Collectors.toList());
        when(mockClient.listColumns(any(), any(String.class), any(String.class), anyInt(), anyMap(), isNull())).thenReturn(columnsList);
        when(session.getProperty(eq("client_page_size"), eq(Integer.class))).thenReturn(5);

        VastMetadata unit = new VastMetadata(mockClient, null, null);
        VastTableHandle tableHandle = new VastTableHandle("schema", "table", "id", false);
        assertNull(tableHandle.getColumnHandlesCache());
        ConnectorTableSchema tableSchema1 = unit.getTableSchema(session, tableHandle);
        assertEquals(tableHandle.getColumnHandlesCache(), columnHandlesList);
        ConnectorTableSchema tableSchema2 = unit.getTableSchema(session, tableHandle);
        assertEquals(tableSchema1.getColumns(), tableSchema2.getColumns());
        assertEquals(tableSchema2.getColumns().size(), columnHandlesList.size());
        ConnectorTableSchema tableSchema3 = unit.getTableSchema(session, tableHandle.forDelete());
        assertEquals(tableSchema1.getColumns(), tableSchema3.getColumns());
        assertEquals(tableSchema3.getColumns().size(), columnHandlesList.size());
        verify(mockClient, times(1)).listColumns(any(), any(String.class), any(String.class), anyInt(), anyMap(), isNull());
    }

    @Test
    public void testGetTableSchemaForImport()
            throws VastException
    {
        Field field1 = new Field("col1", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        Field field2 = new Field("col2", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        List<Field> columnsList = List.of(field1, field2);
        List<VastColumnHandle> columnHandlesList = columnsList.stream().map(VastColumnHandle::fromField).collect(Collectors.toList());
        when(mockClient.listColumns(any(), any(String.class), any(String.class), anyInt(), anyMap(), isNull())).thenReturn(columnsList);
        when(session.getProperty(eq("client_page_size"), eq(Integer.class))).thenReturn(5);

        VastMetadata unit = new VastMetadata(mockClient, null, null);
        VastTableHandle tableHandle = new VastTableHandle("schema", "table", "id", true);
        assertNull(tableHandle.getColumnHandlesCache());
        ConnectorTableSchema tableSchema1 = unit.getTableSchema(session, tableHandle);
        assertEquals(tableHandle.getColumnHandlesCache(), columnHandlesList);
        ConnectorTableSchema tableSchema2 = unit.getTableSchema(session, tableHandle);
        assertEquals(tableSchema1.getColumns(), tableSchema2.getColumns());
        assertEquals(tableSchema2.getColumns().size(), columnHandlesList.size() + 1);
        verify(mockClient, times(1)).listColumns(any(), any(String.class), any(String.class), anyInt(), anyMap(), isNull());
    }

    @Test
    public void testApplyFilter_ScalarColumnsOnly()
    {
        // Test that only scalar columns (no children) are supported for predicate pushdown
        VastColumnHandle stringColumn = VastColumnHandle.fromField(Field.nullable("string_col", ArrowType.Utf8.INSTANCE));
        Field arrayElementField = Field.nullable("element", ArrowType.Utf8.INSTANCE);
        Field arrayField = new Field("array_col", FieldType.nullable(new ArrowType.List()), List.of(arrayElementField));
        VastColumnHandle arrayColumn = VastColumnHandle.fromField(arrayField);

        Map<VastColumnHandle, Domain> domains = new HashMap<>();
        domains.put(stringColumn, singleValue(VARCHAR, utf8Slice("test")));
        domains.put(arrayColumn, singleValue(VARCHAR, utf8Slice("test"))); // This should be filtered out

        Constraint constraint = createConstraint(domains);

        VastTableHandle tableHandle = new VastTableHandle("schema", "table", "id", false);
        VastMetadata metadata = new VastMetadata(mockClient, null, null);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(session, tableHandle, constraint);

        assertTrue(result.isPresent());
        ConstraintApplicationResult<ConnectorTableHandle> applicationResult = result.orElseThrow();

        // Should only have scalar columns in enforced predicate
        VastTableHandle newTableHandle = (VastTableHandle) applicationResult.getHandle();
        TupleDomain<VastColumnHandle> enforcedPredicate = newTableHandle.getPredicate();
        assertTrue(enforcedPredicate.getDomains().isPresent());

        Set<VastColumnHandle> enforcedColumns = enforcedPredicate.getDomains().orElseThrow().keySet();
        assertTrue(enforcedColumns.contains(stringColumn));
        assertFalse(enforcedColumns.contains(arrayColumn)); // Array column should be filtered out
    }

    @Test
    public void testApplyFilter_OnlyOrderedPushdownFalse()
    {
        // Test with only_ordered_pushdown=false (default behavior) - should work with scalar columns
        VastColumnHandle stringColumn = VastColumnHandle.fromField(Field.nullable("string_col", ArrowType.Utf8.INSTANCE));

        Map<VastColumnHandle, Domain> domains = new HashMap<>();
        domains.put(stringColumn, singleValue(VARCHAR, utf8Slice("test")));

        Constraint constraint = createConstraint(domains);

        VastTableHandle tableHandle = new VastTableHandle("schema", "table", "id", false);
        VastMetadata metadata = new VastMetadata(mockClient, null, null);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(session, tableHandle, constraint);

        // Should succeed because only_ordered_pushdown=false allows scalar columns regardless of sorting
        assertTrue(result.isPresent());
        ConstraintApplicationResult<ConnectorTableHandle> applicationResult = result.orElseThrow();

        VastTableHandle newTableHandle = (VastTableHandle) applicationResult.getHandle();
        TupleDomain<VastColumnHandle> enforcedPredicate = newTableHandle.getPredicate();
        assertTrue(enforcedPredicate.getDomains().isPresent());

        Set<VastColumnHandle> enforcedColumns = enforcedPredicate.getDomains().orElseThrow().keySet();
        assertTrue(enforcedColumns.contains(stringColumn));
    }

    @Test
    public void testApplyFilter_OnlyOrderedPushdownTrue()
    {
        // Test with only_ordered_pushdown=true - should fail because column is not sorted
        VastColumnHandle stringColumn = VastColumnHandle.fromField(Field.nullable("string_col", ArrowType.Utf8.INSTANCE));

        Map<VastColumnHandle, Domain> domains = new HashMap<>();
        domains.put(stringColumn, singleValue(VARCHAR, utf8Slice("test")));

        Constraint constraint = createConstraint(domains);

        VastTableHandle tableHandle = new VastTableHandle("schema", "table", "id", false);
        VastMetadata metadata = new VastMetadata(mockClient, null, null);

        // Mock session with only_ordered_pushdown=true
        when(session.getProperty(eq("only_ordered_pushdown"), eq(Boolean.class))).thenReturn(true);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(session, tableHandle, constraint);

        // Should return empty because no columns can be pushed down (column is not sorted)
        assertFalse(result.isPresent());
    }

    @Test
    public void testApplyFilter_OnlyOrderedPushdownWithMixedColumns()
    {
        // Test with only_ordered_pushdown=true - table has 4 columns: 2 sorted, 2 non-sorted
        // Predicates on both sorted and non-sorted columns - only sorted should be pushed down

        // Create 4 columns: 2 sorted, 2 non-sorted
        VastColumnHandle sortedColumn1 = VastColumnHandle.fromField(Field.nullable("sorted_col1", ArrowType.Utf8.INSTANCE));
        VastColumnHandle sortedColumn2 = VastColumnHandle.fromField(Field.nullable("sorted_col2", new ArrowType.Int(32, true)));
        VastColumnHandle nonSortedColumn1 = VastColumnHandle.fromField(Field.nullable("non_sorted_col1", ArrowType.Utf8.INSTANCE));
        VastColumnHandle nonSortedColumn2 = VastColumnHandle.fromField(Field.nullable("non_sorted_col2", new ArrowType.Int(32, true)));

        // Create predicates on all 4 columns
        Map<VastColumnHandle, Domain> domains = new HashMap<>();
        domains.put(sortedColumn1, singleValue(VARCHAR, utf8Slice("sorted_value1")));
        domains.put(sortedColumn2, singleValue(io.trino.spi.type.IntegerType.INTEGER, 100L));
        domains.put(nonSortedColumn1, singleValue(VARCHAR, utf8Slice("non_sorted_value1")));
        domains.put(nonSortedColumn2, singleValue(io.trino.spi.type.IntegerType.INTEGER, 200L));

        Constraint constraint = createConstraint(domains);

        // Create table handle with sorted columns
        VastTableHandle tableHandle = new VastTableHandle("schema", "table", "id", false);
        // Mock the sorted columns for this table
        try {
            when(mockClient.listSortedColumns(isNull(), any(String.class), any(String.class), anyInt(), isNull()))
                    .thenReturn(List.of(
                            Field.nullable("sorted_col1", ArrowType.Utf8.INSTANCE),
                            Field.nullable("sorted_col2", new ArrowType.Int(32, true))));
        }
        catch (VastException e) {
            // This won't happen in the test, but needed for compilation
            throw new RuntimeException(e);
        }

        VastMetadata metadata = new VastMetadata(mockClient, null, null);

        // Mock session with only_ordered_pushdown=true
        when(session.getProperty(eq("only_ordered_pushdown"), eq(Boolean.class))).thenReturn(true);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(session, tableHandle, constraint);

        // Verify that the mock was called
        try {
            verify(mockClient, atLeastOnce()).listSortedColumns(isNull(), any(String.class), any(String.class), anyInt(), isNull());
        }
        catch (VastException e) {
            // This won't happen in the test, but needed for compilation
            throw new RuntimeException(e);
        }

        // Should succeed and only have sorted columns in enforced predicate
        assertTrue(result.isPresent());
        ConstraintApplicationResult<ConnectorTableHandle> applicationResult = result.orElseThrow();

        VastTableHandle newTableHandle = (VastTableHandle) applicationResult.getHandle();
        TupleDomain<VastColumnHandle> enforcedPredicate = newTableHandle.getPredicate();
        assertTrue(enforcedPredicate.getDomains().isPresent());

        Set<VastColumnHandle> enforcedColumns = enforcedPredicate.getDomains().orElseThrow().keySet();

        // Only sorted columns should be in enforced predicate
        assertTrue(enforcedColumns.contains(sortedColumn1));
        assertTrue(enforcedColumns.contains(sortedColumn2));
        assertFalse(enforcedColumns.contains(nonSortedColumn1));
        assertFalse(enforcedColumns.contains(nonSortedColumn2));
    }

    @Test
    public void testApplyFilter_NoChangesNeeded()
    {
        // Test when no changes are needed (same predicate as existing table) - using integer type
        VastColumnHandle intColumn = VastColumnHandle.fromField(Field.nullable("int_col", new ArrowType.Int(32, true)));

        Map<VastColumnHandle, Domain> domains = new HashMap<>();
        domains.put(intColumn, singleValue(io.trino.spi.type.IntegerType.INTEGER, 42L));

        Constraint constraint = createConstraint(domains);

        // Create table handle with the same predicate
        TupleDomain<VastColumnHandle> existingPredicate = TupleDomain.withColumnDomains(domains);
        VastTableHandle tableHandle = new VastTableHandle("schema", "table", "id", false)
                .withPredicate(existingPredicate, Optional.empty(), List.of());

        VastMetadata metadata = new VastMetadata(mockClient, null, null);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(session, tableHandle, constraint);

        // Should return empty when no changes are needed
        assertFalse(result.isPresent());
    }

    @Test
    public void testApplyFilter_EmptyConstraint()
    {
        // Test with empty constraint
        TupleDomain<ColumnHandle> emptySummary = TupleDomain.all();
        Constraint constraint = new Constraint(emptySummary);

        VastTableHandle tableHandle = new VastTableHandle("schema", "table", "id", false);
        VastMetadata metadata = new VastMetadata(mockClient, null, null);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(session, tableHandle, constraint);

        // Should return empty when no constraints to apply
        assertFalse(result.isPresent());
    }

    @Test
    public void testApplyFilter_WithExistingPredicate()
    {
        // Test applying new predicate on table with existing predicate
        VastColumnHandle stringColumn = VastColumnHandle.fromField(Field.nullable("string_col", ArrowType.Utf8.INSTANCE));

        // Existing predicate
        Map<VastColumnHandle, Domain> existingDomains = new HashMap<>();
        existingDomains.put(stringColumn, singleValue(VARCHAR, utf8Slice("existing")));
        TupleDomain<VastColumnHandle> existingPredicate = TupleDomain.withColumnDomains(existingDomains);

        // New constraint - use a range that intersects with existing
        Map<VastColumnHandle, Domain> newDomains = new HashMap<>();
        newDomains.put(stringColumn, singleValue(VARCHAR, utf8Slice("existing")));
        Constraint constraint = createConstraint(newDomains);

        VastTableHandle tableHandle = new VastTableHandle("schema", "table", "id", false)
                .withPredicate(existingPredicate, Optional.empty(), List.of());

        VastMetadata metadata = new VastMetadata(mockClient, null, null);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(session, tableHandle, constraint);

        // Should return empty when predicates are the same
        assertFalse(result.isPresent());
    }

    @Test
    public void testApplyFilter_NestedStructScalarFields()
    {
        // Test predicate on leaf fields (a and b) within a nested struct column
        // Create a struct with leaf fields a and b
        Field fieldA = Field.nullable("a", ArrowType.Utf8.INSTANCE);
        Field fieldB = Field.nullable("b", new ArrowType.Int(32, true));
        Field structField = new Field("some_struct", FieldType.nullable(new ArrowType.Struct()), List.of(fieldA, fieldB));
        // Create column handles for the leaf fields within the struct
        VastColumnHandle leafColumnA = VastColumnHandle.fromField(fieldA);
        VastColumnHandle leafColumnB = VastColumnHandle.fromField(fieldB);

        Map<VastColumnHandle, Domain> domains = new HashMap<>();
        domains.put(leafColumnA, singleValue(VARCHAR, utf8Slice("test_a")));
        domains.put(leafColumnB, singleValue(io.trino.spi.type.IntegerType.INTEGER, 42L));

        Constraint constraint = createConstraint(domains);

        VastTableHandle tableHandle = new VastTableHandle("schema", "table", "id", false);
        VastMetadata metadata = new VastMetadata(mockClient, null, null);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(session, tableHandle, constraint);

        // Should succeed because leaf fields (a and b) within nested structs are supported for predicate pushdown
        assertTrue(result.isPresent());
        ConstraintApplicationResult<ConnectorTableHandle> applicationResult = result.orElseThrow();

        // Should have both leaf columns in enforced predicate
        VastTableHandle newTableHandle = (VastTableHandle) applicationResult.getHandle();
        TupleDomain<VastColumnHandle> enforcedPredicate = newTableHandle.getPredicate();
        assertTrue(enforcedPredicate.getDomains().isPresent());

        Set<VastColumnHandle> enforcedColumns = enforcedPredicate.getDomains().orElseThrow().keySet();
        assertTrue(enforcedColumns.contains(leafColumnA));
        assertTrue(enforcedColumns.contains(leafColumnB));
    }

    @Test
    public void testApplyFilter_StringPrefixPredicate()
    {
        // Test string prefix predicate (LIKE 'prefix%') using range domain
        VastColumnHandle stringColumn = VastColumnHandle.fromField(Field.nullable("string_col", ArrowType.Utf8.INSTANCE));

        // Create a range domain that represents "prefix%" (>= 'prefix' AND < 'prefiy')
        Domain prefixDomain = Domain.create(
                io.trino.spi.predicate.ValueSet.ofRanges(
                        io.trino.spi.predicate.Range.range(VARCHAR, utf8Slice("prefix"), true, utf8Slice("prefiy"), false)),
                false);
        Map<VastColumnHandle, Domain> domains = new HashMap<>();
        domains.put(stringColumn, prefixDomain);

        Constraint constraint = createConstraint(domains);

        VastTableHandle tableHandle = new VastTableHandle("schema", "table", "id", false);
        VastMetadata metadata = new VastMetadata(mockClient, null, null);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(session, tableHandle, constraint);

        assertTrue(result.isPresent());
        ConstraintApplicationResult<ConnectorTableHandle> applicationResult = result.orElseThrow();

        // Should have string column in enforced predicate
        VastTableHandle newTableHandle = (VastTableHandle) applicationResult.getHandle();
        TupleDomain<VastColumnHandle> enforcedPredicate = newTableHandle.getPredicate();
        assertTrue(enforcedPredicate.getDomains().isPresent());

        Set<VastColumnHandle> enforcedColumns = enforcedPredicate.getDomains().orElseThrow().keySet();
        assertTrue(enforcedColumns.contains(stringColumn));
    }
}
