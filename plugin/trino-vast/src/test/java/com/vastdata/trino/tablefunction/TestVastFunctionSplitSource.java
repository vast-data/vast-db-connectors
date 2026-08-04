/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.tablefunction;

import com.vastdata.client.queryengine.VastQueryEngineClient;
import com.vastdata.client.schema.ArrowSchemaUtils;
import com.vastdata.trino.tx.VastTransactionHandle;
import com.vastdata.trino.tx.VastTrinoTransactionHandleManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import vastdb.queryengine.protocol.GetQueryStatusResponse;
import vastdb.queryengine.protocol.StartQueryResponse;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestVastFunctionSplitSource
{
    private VastQueryEngineClient mockVastQEClient;
    private VastTrinoTransactionHandleManager mockTransactionHandleManager;
    private VastTransactionHandle mockTransactionHandle;
    private VastConnectorTableFunctionHandle mockFunctionHandle;
    private ConnectorSession mockSession;
    private VastFunctionSplitSource splitSource;
    private ArrowSchemaUtils arrowSchemaUtils;

    @BeforeEach
    void setUp()
    {
        this.mockVastQEClient = mock(VastQueryEngineClient.class);
        this.mockTransactionHandleManager = mock(
                VastTrinoTransactionHandleManager.class);
        this.mockTransactionHandle = mock(VastTransactionHandle.class);
        this.arrowSchemaUtils = mock(ArrowSchemaUtils.class);
    }

    @Test
    void testReplaceGroups()
            throws Exception
    {
        mockSession = mock(ConnectorSession.class);
        Set<String> groups = new LinkedHashSet<>();
        groups.add("group1");
        groups.add("group2");
        when(mockSession.getIdentity()).thenReturn(ConnectorIdentity.forUser(
                "testing").withGroups(groups).build());
        mockFunctionHandle = new VastConnectorTableFunctionHandle(
                "SELECT * FROM table WHERE g in [<groups>]", true);
        splitSource = new VastFunctionSplitSource(mockVastQEClient,
                mockTransactionHandleManager, mockTransactionHandle,
                mockFunctionHandle, mockSession, arrowSchemaUtils);
        StartQueryResponse mockResponse = StartQueryResponse
                .newBuilder()
                .build();
        GetQueryStatusResponse mockStatusResponse = GetQueryStatusResponse
                .newBuilder()
                .build();
        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(
                String.class);
        when(mockVastQEClient.startQuery(any(),
                queryCaptor.capture())).thenReturn(mockResponse);
        when(mockVastQEClient.getQueryStatus(any())).thenReturn(
                mockStatusResponse);
        when(arrowSchemaUtils.parseSchema(any(), any())).thenReturn(
                mock(Schema.class));

        splitSource.getNextBatch(10);
        assertThat(queryCaptor.getValue()).containsAnyOf("'group1'",
                "'group2'");
    }

    @Test
    void testNoReplaceWhenForceDisabled()
            throws Exception
    {
        mockSession = mock(ConnectorSession.class);
        verify(mockSession, never()).getIdentity();
        mockFunctionHandle = new VastConnectorTableFunctionHandle(
                "SELECT * FROM table WHERE g in [<groups>]", false);
        splitSource = new VastFunctionSplitSource(mockVastQEClient,
                mockTransactionHandleManager, mockTransactionHandle,
                mockFunctionHandle, mockSession, arrowSchemaUtils);
        StartQueryResponse mockResponse = StartQueryResponse
                .newBuilder()
                .build();
        GetQueryStatusResponse mockStatusResponse = GetQueryStatusResponse
                .newBuilder()
                .build();
        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(
                String.class);
        when(mockVastQEClient.startQuery(any(),
                queryCaptor.capture())).thenReturn(mockResponse);
        when(mockVastQEClient.getQueryStatus(any())).thenReturn(
                mockStatusResponse);
        when(arrowSchemaUtils.parseSchema(any(), any())).thenReturn(
                mock(Schema.class));

        splitSource.getNextBatch(10);
        assertThat(queryCaptor.getValue()).isEqualTo(
                "SELECT * FROM table WHERE g in [<groups>]");
    }
}
