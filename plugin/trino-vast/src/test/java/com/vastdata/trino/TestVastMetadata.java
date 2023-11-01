/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableSchema;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestVastMetadata
{
    @Mock VastClient mockClient;
    @Mock ConnectorSession session;

    private AutoCloseable autoCloseable;

    @BeforeMethod
    public void setup()
    {
        autoCloseable = openMocks(this);
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        autoCloseable.close();
    }

    @Test
    public void testGetTableSchemaNotForImport()
            throws VastException
    {
        Field field1 = new Field("col1", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        Field field2 = new Field("col2", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        List<Field> columnsList = List.of(field1, field2);
        List<VastColumnHandle> columnHandlesList = columnsList.stream().map(VastColumnHandle::fromField).collect(Collectors.toList());
        when(mockClient.listColumns(any(), any(String.class), any(String.class), anyInt())).thenReturn(columnsList);
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
        verify(mockClient, times(1)).listColumns(any(), any(String.class), any(String.class), anyInt());
    }

    @Test
    public void testGetTableSchemaForImport()
            throws VastException
    {
        Field field1 = new Field("col1", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        Field field2 = new Field("col2", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        List<Field> columnsList = List.of(field1, field2);
        List<VastColumnHandle> columnHandlesList = columnsList.stream().map(VastColumnHandle::fromField).collect(Collectors.toList());
        when(mockClient.listColumns(any(), any(String.class), any(String.class), anyInt())).thenReturn(columnsList);
        when(session.getProperty(eq("client_page_size"), eq(Integer.class))).thenReturn(5);

        VastMetadata unit = new VastMetadata(mockClient, null, null);
        VastTableHandle tableHandle = new VastTableHandle("schema", "table", "id", true);
        assertNull(tableHandle.getColumnHandlesCache());
        ConnectorTableSchema tableSchema1 = unit.getTableSchema(session, tableHandle);
        assertEquals(tableHandle.getColumnHandlesCache(), columnHandlesList);
        ConnectorTableSchema tableSchema2 = unit.getTableSchema(session, tableHandle);
        assertEquals(tableSchema1.getColumns(), tableSchema2.getColumns());
        assertEquals(tableSchema2.getColumns().size(), columnHandlesList.size() + 1);
        verify(mockClient, times(1)).listColumns(any(), any(String.class), any(String.class), anyInt());
    }
}
