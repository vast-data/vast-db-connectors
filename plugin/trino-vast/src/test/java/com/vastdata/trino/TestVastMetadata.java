/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.session.PropertyMetadata;
import io.trino.testing.TestingConnectorSession;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.mockito.Mock;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestVastMetadata
{
    @Mock VastClient mockClient;

    private final TestingConnectorSession session = TestingConnectorSession.builder()
            .setPropertyMetadata(List.of(PropertyMetadata.integerProperty("client_page_size", "very interesting", 5, false)))
            .setPropertyValues(Map.of("client_page_size", 5))
            .build();

    @Test
    public void testGetTableSchemaNotForImport()
            throws VastException
    {
        initMocks(this);
        Field field1 = new Field("col1", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        Field field2 = new Field("col2", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        List<Field> columnsList = List.of(field1, field2);
        List<VastColumnHandle> columnHandlesList = columnsList.stream().map(VastColumnHandle::fromField).collect(Collectors.toList());
        when(mockClient.listColumns(any(VastTransactionHandle.class), any(String.class), any(String.class), anyInt())).thenReturn(columnsList);
        VastMetadata unit = new VastMetadata(mockClient, null, null);
        VastTableHandle tableHandle = new VastTableHandle("schema", "table", false);
        assertNull(tableHandle.getColumnHandlesCache());
        ConnectorTableSchema tableSchema1 = unit.getTableSchema(session, tableHandle);
        assertEquals(tableHandle.getColumnHandlesCache(), columnHandlesList);
        ConnectorTableSchema tableSchema2 = unit.getTableSchema(session, tableHandle);
        assertEquals(tableSchema1.getColumns(), tableSchema2.getColumns());
        assertEquals(tableSchema2.getColumns().size(), columnHandlesList.size());
        ConnectorTableSchema tableSchema3 = unit.getTableSchema(session, tableHandle.forDelete());
        assertEquals(tableSchema1.getColumns(), tableSchema3.getColumns());
        assertEquals(tableSchema3.getColumns().size(), columnHandlesList.size());
        verify(mockClient, times(1)).listColumns(any(VastTransaction.class), any(String.class), any(String.class), anyInt());
    }

    @Test
    public void testGetTableSchemaForImport()
            throws VastException
    {
        initMocks(this);
        Field field1 = new Field("col1", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        Field field2 = new Field("col2", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        List<Field> columnsList = List.of(field1, field2);
        List<VastColumnHandle> columnHandlesList = columnsList.stream().map(VastColumnHandle::fromField).collect(Collectors.toList());
        when(mockClient.listColumns(any(VastTransactionHandle.class), any(String.class), any(String.class), anyInt())).thenReturn(columnsList);
        VastMetadata unit = new VastMetadata(mockClient, null, null);
        VastTableHandle tableHandle = new VastTableHandle("schema", "table", true);
        assertNull(tableHandle.getColumnHandlesCache());
        ConnectorTableSchema tableSchema1 = unit.getTableSchema(session, tableHandle);
        assertEquals(tableHandle.getColumnHandlesCache(), columnHandlesList);
        ConnectorTableSchema tableSchema2 = unit.getTableSchema(session, tableHandle);
        assertEquals(tableSchema1.getColumns(), tableSchema2.getColumns());
        assertEquals(tableSchema2.getColumns().size(), columnHandlesList.size() + 1);
        verify(mockClient, times(1)).listColumns(any(VastTransaction.class), any(String.class), any(String.class), anyInt());
    }
}
