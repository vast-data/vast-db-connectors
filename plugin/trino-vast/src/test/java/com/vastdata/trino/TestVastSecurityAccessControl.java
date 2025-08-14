/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.vastdata.client.RowColumnSecurityResponse;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastClientForTests;
import com.vastdata.client.error.VastServerException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.tx.VastAutocommitTransaction;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.mockserver.MockMapSchema;
import com.vastdata.mockserver.MockUtils;
import com.vastdata.mockserver.VastMockS3Server;
import com.vastdata.mockserver.VastRootHandler;
import com.vastdata.trino.tx.VastTransactionHandleFactory;
import com.vastdata.trino.tx.VastTrinoTransactionHandleManager;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static com.vastdata.client.VastClient.AUDIT_LOG_BUCKET_NAME;
import static com.vastdata.client.VastClient.BIG_CATALOG_BUCKET_NAME;
import static com.vastdata.trino.tx.VastTrinoTransactionHandleManager.ALWAYS_EMPTY_TRANSACTION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertThrows;

public class TestVastSecurityAccessControl
{
    public static final String CATALOG_NAME = "vast";
    public static final String TEST_BUCKET = "buck";
    private static VastMockS3Server mockServer;
    private static final VastRootHandler handler = new VastRootHandler();
    @Mock VastTransaction mockTransactionHandle;
    @Mock SystemSecurityContext ctxMock;
    private static AutoCloseable autoCloseable;
    private static VastClient vastClient;
    private static final Identity IDENTITY_1 = Identity.forUser("user1").build();

    static {
        VastAutocommitTransaction.alterTransaction = ALWAYS_EMPTY_TRANSACTION;
    }

    @BeforeAll
    public static void startMockServer()
            throws IOException
    {
        mockServer = new VastMockS3Server(0, handler);
        int testPort = mockServer.start();
        vastClient = VastClientForTests.getVastClient(new JettyHttpClient(), testPort);
        MockUtils mockUtils = new MockUtils();
        mockUtils.createBucket(testPort, TEST_BUCKET);
    }

    @AfterAll
    public static void stopServer()
            throws Exception
    {
        if (Objects.nonNull(mockServer)) {
            mockServer.close();
        }
    }

    @BeforeEach
    public void clearMockServer()
    {
        Map<String, Set<MockMapSchema>> testMockServerSchema = new HashMap<>(1);
        testMockServerSchema.put(AUDIT_LOG_BUCKET_NAME, ImmutableSet.of());
        testMockServerSchema.put(BIG_CATALOG_BUCKET_NAME, ImmutableSet.of());
        handler.setSchema(testMockServerSchema);
        autoCloseable = openMocks(this);
        when(mockTransactionHandle.getId()).thenReturn(Long.parseUnsignedLong("514026084031791104"));
        doReturn(IDENTITY_1).when(ctxMock).getIdentity();
    }

    @AfterEach
    public void teardown()
    {
        try {
            autoCloseable.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Object[][] provideValuesPositiveTest()
    {
        final Object[][] shouldEnableRowColumnSecurity = new Boolean[][] {{false}, {true}};
        final Object[][] shouldEnableEndUserImpersonation = new Boolean[][] {{false}, {true}};
        final Object[][] args = new Object[][] {
                {ImmutableList.of(), ImmutableSet.of(), ImmutableSet.of(), ImmutableMap.of()},
                {ImmutableList.of("column1", "column2", "column3"), ImmutableSet.of(), ImmutableSet.of(), ImmutableMap.of()},
                {ImmutableList.of(), ImmutableSet.of(), ImmutableSet.of(), ImmutableMap.of("column2", "somemask")},
        };
        return cartesianProduct(
                cartesianProduct(shouldEnableRowColumnSecurity, shouldEnableEndUserImpersonation),
                args);
    }

    public static Object[][] provideValuesNegativeTest()
    {
        final Object[][] shouldEnableEndUserImpersonation = new Boolean[][] {{false}, {true}};
        final Object[][] args = new Object[][] {
                {ImmutableList.of(), ImmutableSet.of(), ImmutableSet.of("column2"), ImmutableMap.of()},
                {ImmutableList.of(), ImmutableSet.of("column1", "column3"), ImmutableSet.of("column2"), ImmutableMap.of()},
                {ImmutableList.of(), ImmutableSet.of("column1", "column3"), ImmutableSet.of(), ImmutableMap.of()}
        };
        return cartesianProduct(shouldEnableEndUserImpersonation, args);
    }

    @ParameterizedTest
    @MethodSource("provideValuesPositiveTest")
    public void testCheckCanSelectFromColumnsPositive(final boolean shouldEnableRowColumnSecurity,
                                                      final boolean shouldEnableEndUserImpersonation,
                                                      final List<String> rowFilters,
                                                      final Set<String> allowedColumns,
                                                      final Set<String> deniedColumns,
                                                      final Map<String, String> maskedColumns)
            throws VastServerException, VastUserException
    {
        runCheckCanSelectFromColumnsScenario(shouldEnableRowColumnSecurity, shouldEnableEndUserImpersonation, rowFilters, allowedColumns, deniedColumns, maskedColumns);
    }

    @ParameterizedTest
    @MethodSource("provideValuesNegativeTest")
    public void testCheckCanSelectFromColumnsNegative(final boolean shouldEnableEndUserImpersonation,
                                                      final List<String> rowFilters,
                                                      final Set<String> allowedColumns,
                                                      final Set<String> deniedColumns,
                                                      final Map<String, String> maskedColumns)
            throws VastServerException, VastUserException
    {
        assertThrows(AccessDeniedException.class, () -> runCheckCanSelectFromColumnsScenario(true, shouldEnableEndUserImpersonation, rowFilters, allowedColumns, deniedColumns, maskedColumns));
    }

    @ParameterizedTest
    @MethodSource("provideValuesNegativeTest")
    public void testCheckCanSelectFromColumnsNegativeWithoutRowColumnSecurity(final boolean shouldEnableEndUserImpersonation,
                                                                              final List<String> rowFilters,
                                                                              final Set<String> allowedColumns,
                                                                              final Set<String> deniedColumns,
                                                                              final Map<String, String> maskedColumns)
            throws VastServerException, VastUserException
    {
        runCheckCanSelectFromColumnsScenario(false, shouldEnableEndUserImpersonation, rowFilters, allowedColumns, deniedColumns, maskedColumns);
    }

    private void runCheckCanSelectFromColumnsScenario(final boolean shouldEnableRowColumnSecurity,
                                                      final boolean shouldEnableEndUserImpersonation,
                                                      final List<String> rowFilters,
                                                      final Set<String> allowedColumns,
                                                      final Set<String> deniedColumns,
                                                      final Map<String, String> maskedColumns)
            throws VastServerException, VastUserException
    {
        RowColumnSecurityResponse res = new RowColumnSecurityResponse(rowFilters, allowedColumns, deniedColumns, maskedColumns);
        VastClient spy = spy(vastClient);
        String schemaName = "s1";
        String tableName = "t1";
        doReturn(res).when(spy).getRowColumnSecurity(any(VastAutocommitTransaction.class), anyString(), anyString(), nullable(String.class));
        VastTrinoTransactionHandleManager tm = new VastTrinoTransactionHandleManager(spy, new VastTransactionHandleFactory());
        VastSecurityAccessControl.setup(shouldEnableRowColumnSecurity, shouldEnableEndUserImpersonation);
        VastSecurityAccessControl unit = new VastSecurityAccessControl(spy, tm);
        Set<String> columns = Sets.newHashSet("column1", "column2", "column3");
        CatalogSchemaTableName table = new CatalogSchemaTableName(CATALOG_NAME, schemaName, tableName);
        unit.checkCanSelectFromColumns(ctxMock, table, columns);
    }

    private static Object[][] cartesianProduct(final Object[][] left, final Object[][] right)
    {
        return Arrays.stream(left)
                .flatMap(currentLeft -> Arrays.stream(right)
                        .map(currentRight ->
                                Stream.concat(Arrays.stream(currentLeft), Arrays.stream(currentRight)).toArray()))
                .toArray(Object[][]::new);
    }
}
