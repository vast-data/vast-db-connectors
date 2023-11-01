/* Copyright (C) Vast Data Ltd. */

package com.vastdata.trino.statistics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.client.error.VastServerException;
import com.vastdata.trino.VastColumnHandle;
import com.vastdata.trino.VastTableHandle;
import com.vastdata.trino.VastTrinoDependenciesFactory;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.mockito.Mock;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static com.vastdata.client.importdata.VastImportDataMetadataUtils.BIG_CATALOG_SCHEMA_PREFIX;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.BIG_CATALOG_TABLE_NAME;
import static java.lang.String.format;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTrinoPersistentStatistics
{
    private static final ColumnStatistics DUMMY_COLUMN_STATISTICS = ColumnStatistics.builder()
            .setDataSize(Estimate.of(32.00))
            .setDistinctValuesCount(Estimate.of(10.00))
            .setNullsFraction(Estimate.unknown())
            .setRange(new io.trino.spi.statistics.DoubleRange(1.00, 100000.00))
            .build();
    public static final int RETRY_MAX_COUNT = 1;
    @Mock
    private VastClient mockClient;

    private AutoCloseable autoCloseable;

    @BeforeTest
    public void setup()
    {
        autoCloseable = openMocks(this);
    }

    @AfterTest
    public void tearDown()
            throws Exception
    {
        autoCloseable.close();
    }

    private VastConfig getMockServerReadyVastConfig()
    {
        return new VastConfig()
                .setEndpoint(URI.create(format("http://localhost:%d", 1234)))
                .setRegion("us-east-1")
                .setAccessKeyId("pIX3SzyuQVmdrIVZnyy0")
                .setSecretAccessKey("5c5HqW3cDQsUNg68OlhJmq72TM2nZxcP5lR6D1ps")
                .setRetryMaxCount(RETRY_MAX_COUNT)
                .setRetrySleepDuration(1);
    }

    @Test
    public void testSetTableStatistics()
    {
        VastClient client = mockClient;
        VastConfig config = getMockServerReadyVastConfig();
        TrinoPersistentStatistics persistentStatistics = new TrinoPersistentStatistics(client, config);
        VastTableHandle table = new VastTableHandle(BIG_CATALOG_SCHEMA_PREFIX + BIG_CATALOG_SCHEMA_PREFIX,
                BIG_CATALOG_TABLE_NAME, "id", false);
        TableStatistics stats = TableStatistics.builder().setRowCount(Estimate.of(11.0)).build();
        persistentStatistics.setTableStatistics(table, stats);
        Optional<TableStatistics> fetchedStats = persistentStatistics.getTableStatistics(table);
        assertEquals(stats, fetchedStats.orElseThrow());
    }

    @Test
    public void testStatsSerialization()
    {
        Field field = Field.nullable("x", new ArrowType.Int(32, true));
        ColumnHandle colHandle = VastColumnHandle.fromField(field);
        TableStatistics stats = TableStatistics.builder()
                .setRowCount(Estimate.of(11.0))
                .setColumnStatistics(colHandle, DUMMY_COLUMN_STATISTICS)
                .build();
        ObjectMapper mapper = TrinoStatisticsMapper.instance();
        try {
            String tsBuffer = mapper.writeValueAsString(new TrinoSerializableTableStatistics(stats));
            TableStatistics newStats = mapper.readValue(tsBuffer, TrinoSerializableTableStatistics.class).getTableStatistics();
            assertEquals(stats, newStats);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testTableStatisticsSerialization()
            throws JsonProcessingException
    {
        VastClient client = mockClient;
        VastConfig config = getMockServerReadyVastConfig();
        TrinoPersistentStatistics persistentStatistics = new TrinoPersistentStatistics(client, config);
        Field field = Field.nullable("x", new ArrowType.Int(32, true));
        ColumnHandle colHandle = VastColumnHandle.fromField(field);
        TableStatistics stats = TableStatistics.builder()
                .setRowCount(Estimate.of(11.0))
                .setColumnStatistics(colHandle, DUMMY_COLUMN_STATISTICS)
                .build();
        VastTableHandle table = new VastTableHandle("buck/schem", "tab", "id", false);
        ObjectMapper mapper = TrinoStatisticsMapper.instance();
        String tsBuffer = mapper.writeValueAsString(new TrinoSerializableTableStatistics(stats));
        when(mockClient.s3GetObj(anyString(), anyString()))
                .thenReturn(Optional.ofNullable(tsBuffer));
        persistentStatistics.deleteTableStatistics(table);
        Optional<TableStatistics> newStats = persistentStatistics.getTableStatistics(table);
        assertEquals(newStats.orElseGet(TableStatistics::empty), stats);
    }

    @Test
    public void testTableStatisticsFetchRetryOnNetworkError()
    {
        VastConfig config = getMockServerReadyVastConfig();
        VastClient spyClient = spy(new VastClient(null, config, new VastTrinoDependenciesFactory()));
        TrinoPersistentStatistics persistentStatistics = new TrinoPersistentStatistics(spyClient, config, stats -> "{}");
        VastTableHandle table = new VastTableHandle("buck/schem", "tab", "id", false);
        try {
            persistentStatistics.setTableStatistics(table, null);
        }
        catch (VastRuntimeException re) {
            Throwable cause = re.getCause();
            while (cause != null && cause.getCause() != null) {
                cause = cause.getCause();
            }
            assertTrue(cause instanceof VastServerException, format("Cause is: %s", cause));
        }
        verify(spyClient, times(RETRY_MAX_COUNT + 1)).getPutObjectResult(anyString(), anyString(), anyString());

        try {
            persistentStatistics.getTableStatistics(table);
        }
        catch (VastRuntimeException re) {
            Throwable cause = re.getCause();
            while (cause != null && cause.getCause() != null) {
                cause = cause.getCause();
            }
            assertTrue(cause instanceof VastServerException, format("Cause is: %s", cause));
        }
        verify(spyClient, times(RETRY_MAX_COUNT + 1)).getObjectAsString(anyString(), anyString());
    }
}
