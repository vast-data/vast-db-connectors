/* Copyright (C) Vast Data Ltd. */

package com.vastdata.trino.statistics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.trino.VastColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.mockito.Mock;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static com.vastdata.client.importdata.VastImportDataMetadataUtils.BIG_CATALOG_SCHEMA_PREFIX;
import static com.vastdata.client.importdata.VastImportDataMetadataUtils.BIG_CATALOG_TABLE_NAME;
import static java.lang.String.format;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class TestTrinoPersistentStatistics
{
    private static final ColumnStatistics DUMMY_COLUMN_STATISTICS = ColumnStatistics.builder()
            .setDataSize(Estimate.of(32.00))
            .setDistinctValuesCount(Estimate.of(10.00))
            .setNullsFraction(Estimate.unknown())
            .setRange(new io.trino.spi.statistics.DoubleRange(1.00, 100000.00))
            .build();
    @Mock
    private VastClient mockClient;

    private VastConfig getMockServerReadyVastConfig()
    {
        return new VastConfig()
                .setEndpoint(URI.create(format("http://localhost:%d", 1234)))
                .setRegion("us-east-1")
                .setAccessKeyId("pIX3SzyuQVmdrIVZnyy0")
                .setSecretAccessKey("5c5HqW3cDQsUNg68OlhJmq72TM2nZxcP5lR6D1ps")
                .setRetryMaxCount(1)
                .setRetrySleepDuration(1);
    }

    @Test
    public void testSetTableStatistics()
    {
        initMocks(this);
        VastClient client = mockClient;
        VastConfig config = getMockServerReadyVastConfig();
        TrinoPersistentStatistics persistentStatistics = new TrinoPersistentStatistics(client, config);
        String url = "/" + BIG_CATALOG_SCHEMA_PREFIX + BIG_CATALOG_TABLE_NAME;
        TableStatistics stats = TableStatistics.builder().setRowCount(Estimate.of(11.0)).build();
        persistentStatistics.setTableStatistics(url, stats);
        Optional<TableStatistics> fetchedStats = persistentStatistics.getTableStatistics(url);
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
        initMocks(this);
        VastClient client = mockClient;
        VastConfig config = getMockServerReadyVastConfig();
        TrinoPersistentStatistics persistentStatistics = new TrinoPersistentStatistics(client, config);
        Field field = Field.nullable("x", new ArrowType.Int(32, true));
        ColumnHandle colHandle = VastColumnHandle.fromField(field);
        TableStatistics stats = TableStatistics.builder()
                .setRowCount(Estimate.of(11.0))
                .setColumnStatistics(colHandle, DUMMY_COLUMN_STATISTICS)
                .build();
        String url = "/buc/t";
        ObjectMapper mapper = TrinoStatisticsMapper.instance();
        String tsBuffer = null;
        tsBuffer = mapper.writeValueAsString(new TrinoSerializableTableStatistics(stats));
        when(mockClient.s3GetObj(anyString(), anyString()))
                .thenReturn(Optional.ofNullable(tsBuffer));
        persistentStatistics.deleteTableStatistics(url);
        Optional<TableStatistics> newStats = persistentStatistics.getTableStatistics(url);
        assertEquals(newStats.orElseGet(TableStatistics::empty), stats);
    }
}
