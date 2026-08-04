/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastObjectDetails;
import com.vastdata.client.partition.PartitionColumnMetadata;
import com.vastdata.client.partition.PartitionConstants;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.trino.metrics.SplitSourceMetrics;
import com.vastdata.trino.statistics.VastStatisticsManager;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.vastdata.client.schema.TestVastMetadataUtils.createObjectDetails;
import static com.vastdata.trino.VastSessionProperties.DYNAMIC_FILTERING_WAIT_TIMEOUT;
import static com.vastdata.trino.VastSessionProperties.DYNAMIC_FILTERING_WAIT_TIMEOUT_FACTOR;
import static com.vastdata.trino.VastSessionProperties.DYNAMIC_FILTER_COMPACTION_THRESHOLD;
import static com.vastdata.trino.VastSplitSource.MAX_DF_RETRY_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestVastSplitSource
{
    private static final int DYNAMIC_FILTERING_WAIT_TIMEOUT_MS = 5000;
    private static final int TEST_DYNAMIC_FILTERING_WAIT_TIMEOUT_FACTOR = 2;

    private NodeManager nodeManager;
    private VastPageSourceProvider vastPageSourceProvider;
    private VastStatisticsManager statisticsManager;
    private VastClient client;
    private VastTransaction tx;
    private ConnectorSession session;
    private VastTableHandle tableHandle;
    private DynamicFilter dynamicFilter;

    private VastSplitSource splitSource;

    @BeforeEach
    void setUp()
    {
        session = mock(ConnectorSession.class);
        nodeManager = mock(NodeManager.class);
        vastPageSourceProvider = mock(VastPageSourceProvider.class);
        statisticsManager = mock(VastStatisticsManager.class);
        client = mock(VastClient.class);
        tx = new VastTransactionHandle(3);
        VastObjectDetails objectDetails = createObjectDetails("table", "id");

        tableHandle = new VastTableHandle("test_schema", "test_table", objectDetails,
                false, false);
        dynamicFilter = mock(DynamicFilter.class);
        when(dynamicFilter.isBlocked()).thenReturn(new CompletableFuture<>());
        when(session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT,
                Integer.class)).thenReturn(DYNAMIC_FILTERING_WAIT_TIMEOUT_MS);
        when(session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT_FACTOR,
                Integer.class)).thenReturn(
                TEST_DYNAMIC_FILTERING_WAIT_TIMEOUT_FACTOR);
        when(session.getProperty(DYNAMIC_FILTER_COMPACTION_THRESHOLD,
                Integer.class)).thenReturn(1000); // value in vast config
    }

    private void buildSplitSource()
    {
        splitSource = new VastSplitSource(nodeManager, vastPageSourceProvider,
                statisticsManager, client, new SplitSourceMetrics(), tx,
                session, tableHandle, dynamicFilter)
        {
            @Override
            protected void createSplits()
            {
            }

            @Override
            public boolean isSplitSourceFinished()
            {
                return true;
            }
        };
    }

    @Test
    void testCalculateWaitTimeMillis_dynamicFilterNotAwaitable()
    {
        when(dynamicFilter.isAwaitable()).thenReturn(false);
        buildSplitSource();
        assertThat(splitSource.getDynamicFilterContext().waitTime).isEqualTo(0);
        assertThat(
                splitSource.getDynamicFilterContext().shouldWait()).isEqualTo(
                false);
    }

    @Test
    void testCalculateWaitTimeMillis_noCoveredColumns()
    {
        when(dynamicFilter.isAwaitable()).thenReturn(true);
        when(dynamicFilter.getColumnsCovered()).thenReturn(
                Collections.emptySet());
        buildSplitSource();
        assertThat(splitSource.getDynamicFilterContext().waitTime).isEqualTo(
                DYNAMIC_FILTERING_WAIT_TIMEOUT_MS);
        assertThat(
                splitSource.getDynamicFilterContext().shouldWait()).isEqualTo(
                true);
        splitSource.getNextBatch(10);
        assertThat(
                splitSource.getDynamicFilterContext().shouldWait()).isEqualTo(
                false);
    }

    @Test
    void testCalculateWaitTimeMillis_sortedColumn()
    {
        when(dynamicFilter.isAwaitable()).thenReturn(true);
        VastColumnHandle col1 = new VastColumnHandle(
                Field.nullable("col1", ArrowType.Utf8.INSTANCE));
        when(dynamicFilter.getColumnsCovered()).thenReturn(Set.of(col1));
        tableHandle = tableHandle.withSortedColumns(List.of("col1"));
        buildSplitSource();
        assertThat(splitSource.getDynamicFilterContext().waitTime).isEqualTo(
                TEST_DYNAMIC_FILTERING_WAIT_TIMEOUT_FACTOR * DYNAMIC_FILTERING_WAIT_TIMEOUT_MS);
        assertThat(
                splitSource.getDynamicFilterContext().shouldWait()).isEqualTo(
                true);
        splitSource.getNextBatch(10);
        assertThat(
                splitSource.getDynamicFilterContext().shouldWait()).isEqualTo(
                true);
        splitSource.getNextBatch(10);
        splitSource.getNextBatch(10);
        splitSource.getNextBatch(10);
        splitSource.getNextBatch(10);
        assertThat(splitSource.getDynamicFilterContext().retryCount).isEqualTo(
                MAX_DF_RETRY_COUNT);
        assertThat(
                splitSource.getDynamicFilterContext().shouldWait()).isEqualTo(
                false);
    }

    @Test
    void testCalculateWaitTimeMillis_sortedColumnCompleteBeforeMaxRetry()
    {
        when(dynamicFilter.isAwaitable()).thenReturn(true);
        VastColumnHandle col1 = new VastColumnHandle(
                Field.nullable("col1", ArrowType.Utf8.INSTANCE));
        when(dynamicFilter.getColumnsCovered()).thenReturn(Set.of(col1));
        tableHandle = tableHandle.withSortedColumns(List.of("col1"));
        buildSplitSource();
        assertThat(splitSource.getDynamicFilterContext().waitTime).isEqualTo(
                TEST_DYNAMIC_FILTERING_WAIT_TIMEOUT_FACTOR * DYNAMIC_FILTERING_WAIT_TIMEOUT_MS);
        assertThat(
                splitSource.getDynamicFilterContext().shouldWait()).isEqualTo(
                true);
        splitSource.getNextBatch(10);
        assertThat(
                splitSource.getDynamicFilterContext().shouldWait()).isEqualTo(
                true);
        splitSource.getNextBatch(10);
        when(dynamicFilter.isComplete()).thenReturn(true);
        assertThat(
                splitSource.getDynamicFilterContext().shouldWait()).isEqualTo(
                false);
    }

    @Test
    void testCalculateWaitTimeMillis_partitionColumn()
    {
        when(dynamicFilter.isAwaitable()).thenReturn(true);
        VastColumnHandle col1 = new VastColumnHandle(
                Field.nullable("col1", ArrowType.Utf8.INSTANCE));
        when(dynamicFilter.getColumnsCovered()).thenReturn(Set.of(col1));
        PartitionColumnMetadata partitionColumn = new PartitionColumnMetadata(
                "col1", "utf-8", "col1", "utf-8",
                PartitionConstants.IDENTITY_TRANSFORM, null);
        tableHandle = tableHandle.withPartitionColumns(
                List.of(partitionColumn));
        buildSplitSource();
        assertThat(splitSource.getDynamicFilterContext().waitTime).isEqualTo(
                TEST_DYNAMIC_FILTERING_WAIT_TIMEOUT_FACTOR * DYNAMIC_FILTERING_WAIT_TIMEOUT_MS);
        assertThat(
                splitSource.getDynamicFilterContext().waitOnce.orElseThrow()).isEqualTo(
                false); // row_count / distinct_values_count = 2, so wait time is 2 * 5000 = 10000
    }

    @Test
    void testCalculateWaitTimeMillis_noStatistics()
    {
        when(dynamicFilter.isAwaitable()).thenReturn(true);
        VastColumnHandle col1 = new VastColumnHandle(
                Field.nullable("col1", ArrowType.Utf8.INSTANCE));
        when(dynamicFilter.getColumnsCovered()).thenReturn(Set.of(col1));
        when(statisticsManager.getTableStatistics(tableHandle)).thenReturn(
                Optional.empty());
        buildSplitSource();
        assertThat(
                splitSource.getDynamicFilterContext().waitTime).isLessThanOrEqualTo(
                DYNAMIC_FILTERING_WAIT_TIMEOUT_MS);
        assertThat(
                splitSource.getDynamicFilterContext().waitOnce.orElseThrow()).isEqualTo(
                true);
    }

    @Test
    void testCalculateWaitTimeMillis_withStatisticsLowSelectivity()
    {
        when(dynamicFilter.isAwaitable()).thenReturn(true);
        VastColumnHandle col1 = new VastColumnHandle(
                Field.nullable("col1", ArrowType.Utf8.INSTANCE));
        when(dynamicFilter.getColumnsCovered()).thenReturn(Set.of(col1));

        TableStatistics tableStatistics = TableStatistics.builder().setRowCount(
                Estimate.of(1000)).setColumnStatistics(col1, ColumnStatistics
                .builder()
                .setDistinctValuesCount(Estimate.of(50))
                .build()).build();
        when(statisticsManager.getTableStatistics(tableHandle)).thenReturn(
                Optional.of(tableStatistics));
        buildSplitSource();
        // (50 / 1000) * 5000 = 250
        assertThat(splitSource.getDynamicFilterContext().waitTime).isEqualTo(
                100000L);
    }

    @Test
    void testCalculateWaitTimeMillis_withStatisticsHighSelectivity()
    {
        when(dynamicFilter.isAwaitable()).thenReturn(true);
        VastColumnHandle col1 = new VastColumnHandle(
                Field.nullable("col1", ArrowType.Utf8.INSTANCE));
        when(dynamicFilter.getColumnsCovered()).thenReturn(Set.of(col1));

        TableStatistics tableStatistics = TableStatistics.builder().setRowCount(
                Estimate.of(1000)).setColumnStatistics(col1, ColumnStatistics
                .builder()
                .setDistinctValuesCount(Estimate.of(500))
                .build()).build();
        when(statisticsManager.getTableStatistics(tableHandle)).thenReturn(
                Optional.of(tableStatistics));
        buildSplitSource();
        // rowCount / distinctValuesCount = 1000 / 500 = 2, which is not > 10, so it should return 0
        assertThat(splitSource.getDynamicFilterContext().waitTime).isEqualTo(
                2 * DYNAMIC_FILTERING_WAIT_TIMEOUT_MS); // row_count / distinct_values_count = 2, so wait time is 2 * 5000 = 10000
        assertThat(
                splitSource.getDynamicFilterContext().waitOnce.orElseThrow()).isEqualTo(
                true);
        assertThat(
                splitSource.getDynamicFilterContext().shouldWait()).isEqualTo(
                true);
        splitSource.getNextBatch(10);
        assertThat(
                splitSource.getDynamicFilterContext().shouldWait()).isEqualTo(
                false);
    }
}
