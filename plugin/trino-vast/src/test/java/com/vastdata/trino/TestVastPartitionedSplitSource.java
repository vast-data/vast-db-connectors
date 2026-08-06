/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.error.VastException;
import com.vastdata.client.partition.PartitionColumnMetadata;
import com.vastdata.client.partition.PartitionConstants;
import com.vastdata.client.tx.VastAutocommitTransaction;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.client.tx.VastTransactionHandleManager;
import com.vastdata.trino.metrics.SplitSourceMetrics;
import com.vastdata.trino.statistics.VastStatisticsManager;
import com.vastdata.trino.tx.VastTransactionHandle;
import io.airlift.slice.Slices;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.Page;
import io.trino.spi.block.ByteArrayBlockBuilder;
import io.trino.spi.block.IntArrayBlockBuilder;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.vastdata.client.partition.PartitionConstants.TRANSFORM_KEY;
import static com.vastdata.client.schema.TestVastMetadataUtils.createObjectDetails;
import static com.vastdata.trino.VastPartitionedSplitSource.TRINO_ROW_ESTIMATE_TRINO_TYPE;
import static com.vastdata.trino.VastSessionProperties.ADAPTIVE_PARTITIONING_PREDICATE;
import static com.vastdata.trino.VastSessionProperties.DATA_ENDPOINTS;
import static com.vastdata.trino.VastSessionProperties.DYNAMIC_FILTERING_WAIT_TIMEOUT;
import static com.vastdata.trino.VastSessionProperties.DYNAMIC_FILTER_COMPACTION_THRESHOLD;
import static com.vastdata.trino.VastSessionProperties.DYNAMIC_FILTER_ELYSIUM_COMPACTION_MULTIPLIER;
import static com.vastdata.trino.VastSessionProperties.DYNAMIC_FILTER_PUSHDOWN_THRESHOLD;
import static com.vastdata.trino.VastSessionProperties.MIN_ROWS_FOR_PARTITION_SPLIT_ESTIMATION;
import static com.vastdata.trino.VastSessionProperties.NUM_OF_SPLITS;
import static com.vastdata.trino.VastSessionProperties.NUM_OF_SUBSPLITS;
import static com.vastdata.trino.VastSessionProperties.QUERY_DATA_ROWS_PER_PAGE;
import static com.vastdata.trino.VastSessionProperties.QUERY_DATA_ROWS_PER_SPLIT;
import static com.vastdata.trino.VastSessionProperties.ROWGROUPS_PER_SUBSPLIT;
import static com.vastdata.trino.tx.VastTrinoTransactionHandleManager.ALWAYS_EMPTY_TRANSACTION;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("resource")
public class TestVastPartitionedSplitSource
{
    private static final Map<String, String> metadata = Map.of(TRANSFORM_KEY,
            "identity");

    VastTransactionHandleManager<VastTransactionHandle> vastTransactionHandleManager;
    private VastStatisticsManager statisticsManager;
    private NodeManager nodeManager;
    private SplitSourceMetrics splitSourceMetrics;

    private static VastClient prepareClient()
    {
        VastClient vastClient = mock(VastClient.class);
        when(vastClient.getSchedulingInfo(any(), any(), any(), any(),
                any())).thenReturn(new VastSchedulingInfo("a"));
        return vastClient;
    }

    private static VastTransactionHandle prepareTx()
    {
        VastTransactionHandle tx = mock(VastTransactionHandle.class);
        when(tx.generateTraceToken(any())).thenReturn(
                new VastTraceToken(Optional.of("aa"), 1, 1));
        return tx;
    }

    private static VastTableHandle prepareTableHandle(
            List<String> sortedColumns, VastColumnHandle... colHandles)
    {
        VastTableHandle tableHandle = new VastTableHandle("buck/schem", "tab",
                createObjectDetails("tab", "id"), false, false);
        tableHandle.setColumnHandlesCache(
                Arrays.stream(colHandles).collect(Collectors.toList()));
        List<PartitionColumnMetadata> partitionColumnMetadataList = sortedColumns
                .stream()
                .map(colName -> new PartitionColumnMetadata(colName,
                        "some-type", colName, "some-type", "Identity",
                        null)) // using colHandles[0] type for simplicity
                .collect(Collectors.toList());
        tableHandle = tableHandle.withPartitionColumns(
                partitionColumnMetadataList);
        return tableHandle;
    }

    private static ConnectorSession prepareSession()
    {
        ConnectorSession session = mock(ConnectorSession.class);
        when(session.getProperty(eq(DYNAMIC_FILTERING_WAIT_TIMEOUT),
                eq(Integer.class))).thenReturn(2);
        when(session.getUser()).thenReturn("test_user");
        when(session.getProperty(eq(DATA_ENDPOINTS),
                eq(List.class))).thenReturn(
                List.of(URI.create("http://localhost")));
        when(session.getProperty(eq(ROWGROUPS_PER_SUBSPLIT),
                eq(Integer.class))).thenReturn(100);
        when(session.getProperty(eq(NUM_OF_SPLITS),
                eq(Integer.class))).thenReturn(1);
        when(session.getProperty(eq(NUM_OF_SUBSPLITS),
                eq(Integer.class))).thenReturn(1);
        when(session.getProperty(eq(QUERY_DATA_ROWS_PER_PAGE),
                eq(Integer.class))).thenReturn(1000);
        when(session.getProperty(eq(QUERY_DATA_ROWS_PER_SPLIT),
                eq(Long.class))).thenReturn(1000L);
        when(session.getProperty(eq(ADAPTIVE_PARTITIONING_PREDICATE),
                eq(Boolean.class))).thenReturn(true);
        when(session.getProperty(eq(DYNAMIC_FILTER_COMPACTION_THRESHOLD),
                eq(Integer.class))).thenReturn(100);
        when(session.getProperty(eq(DYNAMIC_FILTER_PUSHDOWN_THRESHOLD),
                eq(Integer.class))).thenReturn(100);
        when(session.getProperty(
                eq(DYNAMIC_FILTER_ELYSIUM_COMPACTION_MULTIPLIER),
                eq(Integer.class))).thenReturn(100);
        when(session.getProperty(eq(MIN_ROWS_FOR_PARTITION_SPLIT_ESTIMATION),
                eq(Integer.class))).thenReturn(1000000);
        return session;
    }

    @BeforeEach
    public void setup()
    {
        statisticsManager = mock(VastStatisticsManager.class);
        splitSourceMetrics = new SplitSourceMetrics();
        when(statisticsManager.getTableStatistics(any())).thenReturn(
                Optional.of(TableStatistics.empty()));
        VastAutocommitTransaction.alterTransaction = ALWAYS_EMPTY_TRANSACTION;
        vastTransactionHandleManager = mock(VastTransactionHandleManager.class);
        VastTransactionHandle vastTransactionHandle = mock(
                VastTransactionHandle.class);
        when(vastTransactionHandleManager.startTransaction(any())).thenReturn(
                vastTransactionHandle);
        this.nodeManager = mock(NodeManager.class);
        Node node = mock(Node.class);
        when(node.getHostAndPort()).thenReturn(
                HostAddress.fromString("localhost:8080"));
        when(nodeManager.getWorkerNodes()).thenReturn(Set.of(node));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testCallToPartitionTable()
            throws VastException
    {
        ConnectorSession connectorSession = prepareSession();
        VastClient vastClient = prepareClient();
        VastTransaction vastTransaction = prepareTx();
        Field field = new Field("x",
                new FieldType(true, new ArrowType.Utf8(), null, metadata),
                null);
        when(vastClient.listColumns(any(), any(), any(), anyInt(), any(),
                any())).thenReturn(new Schema(List.of(field)));
        VastColumnHandle colHandle = VastColumnHandle.fromField(field);
        VastTableHandle tableHandle = prepareTableHandle(List.of("x"),
                colHandle);
        VastPageSourceProvider vastPageSourceProvider = mock(
                VastPageSourceProvider.class);
        ArgumentCaptor<VastSplit> splitCaptor = ArgumentCaptor.forClass(
                VastSplit.class);
        ArgumentCaptor<VastTableHandle> tableHandleCaptor = ArgumentCaptor.forClass(
                VastTableHandle.class);
        ArgumentCaptor<List<ColumnHandle>> columnHandleCaptor = ArgumentCaptor.forClass(
                List.class);
        when(vastPageSourceProvider.createPageSource(any(), any(),
                splitCaptor.capture(), tableHandleCaptor.capture(),
                columnHandleCaptor.capture(), any())).thenReturn(new EmptyPageSource());
        VastPartitionedSplitSource splitSource = new VastPartitionedSplitSource(
                nodeManager, vastClient, statisticsManager,
                vastPageSourceProvider, vastTransactionHandleManager,
                splitSourceMetrics, vastTransaction, connectorSession,
                tableHandle, DynamicFilter.EMPTY);
        splitSource.getNextBatch(100).join();
        assertThat(tableHandleCaptor.getValue().getTableName()).isEqualTo(
                tableHandle.getTableName() + PartitionConstants.PIT_NAME_SUFFIX);
        columnHandleCaptor
                .getValue()
                .stream()
                .filter(ch -> ((VastColumnHandle) ch)
                        .getField()
                        .getName()
                        .equals("x"))
                .findFirst()
                .orElseThrow();
        assertThat(
                splitCaptor.getValue().getContext().getNumOfSplits()).isEqualTo(
                1); // query on partition table should have 1 split
    }

    @Test
    void testSinglePartitionValue()
            throws VastException
    {
        ConnectorSession connectorSession = prepareSession();
        VastClient vastClient = prepareClient();
        VastTransaction vastTransaction = prepareTx();
        Field fieldX = new Field("x",
                new FieldType(true, new ArrowType.Utf8(), null, metadata),
                null);
        Field fieldY = new Field("y",
                new FieldType(true, new ArrowType.Utf8(), null, metadata),
                null);
        when(vastClient.listColumns(any(), any(), any(), anyInt(), any(),
                any())).thenReturn(new Schema(List.of(fieldX, fieldY)));
        VastColumnHandle colHandle = VastColumnHandle.fromField(fieldX);
        VastTableHandle tableHandle = prepareTableHandle(List.of("x"),
                colHandle);
        VastPageSourceProvider vastPageSourceProvider = mock(
                VastPageSourceProvider.class);
        ConnectorPageSource pageSource = mock(ConnectorPageSource.class);
        when(pageSource.isFinished()).thenReturn(false, true);
        VariableWidthBlockBuilder blockBuilder = VarcharType.createVarcharType(
                8).createBlockBuilder(null, 1);
        blockBuilder.writeEntry(Slices.utf8Slice("part1"));
        LongArrayBlockBuilder rowCountBlockBuilder = createRowEstimateBlock(
                100);
        Page page = new Page(blockBuilder.build(),
                rowCountBlockBuilder.build());
        when(pageSource.getNextSourcePage()).thenReturn(
                SourcePage.create(page));
        when(vastPageSourceProvider.createPageSource(any(), any(), any(), any(),
                any(), any())).thenReturn(pageSource);
        VastPartitionedSplitSource splitSource = new VastPartitionedSplitSource(
                nodeManager, vastClient, statisticsManager,
                vastPageSourceProvider, vastTransactionHandleManager,
                splitSourceMetrics, vastTransaction, connectorSession,
                tableHandle, DynamicFilter.EMPTY);
        List<ConnectorSplit> splits = splitSource
                .getNextBatch(100)
                .join()
                .getSplits();
        assertThat(splits.size()).isEqualTo(1);
        assertThat(splits.getFirst()).isInstanceOf(VastSplit.class);
        VastSplit vastSplit = (VastSplit) splits.getFirst();
        TupleDomain<VastColumnHandle> partitionFilters = vastSplit.getFilters();
        assertThat(partitionFilters.getDomains().isPresent()).isTrue();
        Map<VastColumnHandle, Domain> domains = partitionFilters
                .getDomains()
                .orElseThrow();
        assertThat(domains.size()).isEqualTo(1);
        VastColumnHandle partitionColHandle = domains
                .keySet()
                .iterator()
                .next();
        assertThat(partitionColHandle.getField().getName()).isEqualTo("x");
        Domain domain = domains.get(partitionColHandle);
        assertThat(domain.getValues().getRanges().getRangeCount()).isEqualTo(1);
        assertThat(domain
                .getValues()
                .getRanges()
                .getOrderedRanges()
                .getFirst()
                .isSingleValue()).isEqualTo(true);
        assertThat(domain
                .getValues()
                .getRanges()
                .getOrderedRanges()
                .getFirst()
                .getSingleValue()).isEqualTo(Slices.utf8Slice("part1"));
    }

    @Test
    void testNullPartitionValue()
            throws VastException
    {
        ConnectorSession connectorSession = prepareSession();
        VastClient vastClient = prepareClient();
        VastTransaction vastTransaction = prepareTx();
        Field fieldX = new Field("x",
                new FieldType(true, new ArrowType.Utf8(), null, metadata),
                null);
        Field fieldY = new Field("y",
                new FieldType(true, new ArrowType.Utf8(), null, metadata),
                null);
        when(vastClient.listColumns(any(), any(), any(), anyInt(), any(),
                any())).thenReturn(new Schema(List.of(fieldX, fieldY)));
        VastColumnHandle colHandle = VastColumnHandle.fromField(fieldX);
        VastTableHandle tableHandle = prepareTableHandle(List.of("x"),
                colHandle);
        VastPageSourceProvider vastPageSourceProvider = mock(
                VastPageSourceProvider.class);
        ConnectorPageSource pageSource = mock(ConnectorPageSource.class);
        when(pageSource.isFinished()).thenReturn(false, true);
        VariableWidthBlockBuilder blockBuilder = VarcharType.createVarcharType(
                8).createBlockBuilder(null, 1);
        blockBuilder.appendNull();
        LongArrayBlockBuilder rowCountBlockBuilder = createRowEstimateBlock(
                100);
        Page page = new Page(blockBuilder.build(),
                rowCountBlockBuilder.build());
        when(pageSource.getNextSourcePage()).thenReturn(
                SourcePage.create(page));
        when(vastPageSourceProvider.createPageSource(any(), any(), any(), any(),
                any(), any())).thenReturn(pageSource);
        VastPartitionedSplitSource splitSource = new VastPartitionedSplitSource(
                nodeManager, vastClient, statisticsManager,
                vastPageSourceProvider, vastTransactionHandleManager,
                splitSourceMetrics, vastTransaction, connectorSession,
                tableHandle, DynamicFilter.EMPTY);
        List<ConnectorSplit> splits = splitSource
                .getNextBatch(100)
                .join()
                .getSplits();
        VastSplit vastSplit = (VastSplit) splits.getFirst();
        TupleDomain<VastColumnHandle> partitionFilters = vastSplit.getFilters();
        Map<VastColumnHandle, Domain> domains = partitionFilters
                .getDomains()
                .orElseThrow();
        assertThat(domains.size()).isEqualTo(1);
        VastColumnHandle partitionColHandle = domains
                .keySet()
                .iterator()
                .next();
        assertThat(partitionColHandle.getField().getName()).isEqualTo("x");
        Domain domain = domains.get(partitionColHandle);
        assertThat(domain.isOnlyNull()).isEqualTo(true);
    }

    @Test
    void testSingleTinyIntPartitionValue()
            throws VastException
    {
        ConnectorSession connectorSession = prepareSession();
        VastClient vastClient = prepareClient();
        VastTransaction vastTransaction = prepareTx();
        Field fieldX = new Field("x",
                new FieldType(true, new ArrowType.Int(8, true), null, metadata),
                null);
        when(vastClient.listColumns(any(), any(), any(), anyInt(), any(),
                any())).thenReturn(new Schema(List.of(fieldX)));
        VastColumnHandle colHandle = VastColumnHandle.fromField(fieldX);
        VastTableHandle tableHandle = prepareTableHandle(List.of("x"),
                colHandle);
        VastPageSourceProvider vastPageSourceProvider = mock(
                VastPageSourceProvider.class);
        ConnectorPageSource pageSource = mock(ConnectorPageSource.class);
        when(pageSource.isFinished()).thenReturn(false, true);
        ByteArrayBlockBuilder blockBuilder = (ByteArrayBlockBuilder) TinyintType.TINYINT.createBlockBuilder(
                null, 1);
        blockBuilder.writeByte((byte) 5);
        LongArrayBlockBuilder rowCountBlockBuilder = createRowEstimateBlock(
                100);
        Page page = new Page(blockBuilder.build(),
                rowCountBlockBuilder.build());
        when(pageSource.getNextSourcePage()).thenReturn(
                SourcePage.create(page));
        when(vastPageSourceProvider.createPageSource(any(), any(), any(), any(),
                any(), any())).thenReturn(pageSource);
        VastPartitionedSplitSource splitSource = new VastPartitionedSplitSource(
                nodeManager, vastClient, statisticsManager,
                vastPageSourceProvider, vastTransactionHandleManager,
                splitSourceMetrics, vastTransaction, connectorSession,
                tableHandle, DynamicFilter.EMPTY);
        List<ConnectorSplit> splits = splitSource
                .getNextBatch(100)
                .join()
                .getSplits();
        VastSplit vastSplit = (VastSplit) splits.getFirst();
        TupleDomain<VastColumnHandle> partitionFilters = vastSplit.getFilters();
        Map<VastColumnHandle, Domain> domains = partitionFilters
                .getDomains()
                .orElseThrow();
        VastColumnHandle partitionColHandle = domains
                .keySet()
                .iterator()
                .next();
        Domain domain = domains.get(partitionColHandle);
        assertThat(domain.getValues().getRanges().getRangeCount()).isEqualTo(1);
        assertThat(domain
                .getValues()
                .getRanges()
                .getOrderedRanges()
                .getFirst()
                .isSingleValue()).isEqualTo(true);
        assertThat(domain
                .getValues()
                .getRanges()
                .getOrderedRanges()
                .getFirst()
                .getSingleValue()).isEqualTo(5L);
    }

    private static LongArrayBlockBuilder createRowEstimateBlock(long rowCount)
    {
        LongArrayBlockBuilder rowCountBlockBuilder = (LongArrayBlockBuilder) TRINO_ROW_ESTIMATE_TRINO_TYPE.createBlockBuilder(
                null, 1);
        rowCountBlockBuilder.writeLong(rowCount);
        return rowCountBlockBuilder;
    }

    @Test
    void testSingleDatePartitionValue()
            throws VastException
    {
        ConnectorSession connectorSession = prepareSession();
        VastClient vastClient = prepareClient();
        VastTransaction vastTransaction = prepareTx();
        Field fieldX = new Field("x",
                new FieldType(true, new ArrowType.Date(DateUnit.DAY), null,
                        metadata), null);
        when(vastClient.listColumns(any(), any(), any(), anyInt(), any(),
                any())).thenReturn(new Schema(List.of(fieldX)));
        VastColumnHandle colHandle = VastColumnHandle.fromField(fieldX);
        VastTableHandle tableHandle = prepareTableHandle(List.of("x"),
                colHandle);
        VastPageSourceProvider vastPageSourceProvider = mock(
                VastPageSourceProvider.class);
        ConnectorPageSource pageSource = mock(ConnectorPageSource.class);
        when(pageSource.isFinished()).thenReturn(false, true);
        IntArrayBlockBuilder blockBuilder = (IntArrayBlockBuilder) DateType.DATE.createBlockBuilder(
                null, 1);
        blockBuilder.writeInt(1);
        LongArrayBlockBuilder rowCountBlockBuilder = createRowEstimateBlock(
                200);
        Page page = new Page(blockBuilder.build(),
                rowCountBlockBuilder.build());
        when(pageSource.getNextSourcePage()).thenReturn(
                SourcePage.create(page));
        when(vastPageSourceProvider.createPageSource(any(), any(), any(), any(),
                any(), any())).thenReturn(pageSource);
        VastPartitionedSplitSource splitSource = new VastPartitionedSplitSource(
                nodeManager, vastClient, statisticsManager,
                vastPageSourceProvider, vastTransactionHandleManager,
                splitSourceMetrics, vastTransaction, connectorSession,
                tableHandle, DynamicFilter.EMPTY);
        List<ConnectorSplit> splits = splitSource
                .getNextBatch(100)
                .join()
                .getSplits();
        VastSplit vastSplit = (VastSplit) splits.getFirst();
        TupleDomain<VastColumnHandle> partitionFilters = vastSplit.getFilters();
        Map<VastColumnHandle, Domain> domains = partitionFilters
                .getDomains()
                .orElseThrow();
        VastColumnHandle partitionColHandle = domains
                .keySet()
                .iterator()
                .next();
        Domain domain = domains.get(partitionColHandle);
        assertThat(domain.getValues().getRanges().getRangeCount()).isEqualTo(1);
        assertThat(domain
                .getValues()
                .getRanges()
                .getOrderedRanges()
                .getFirst()
                .isSingleValue()).isEqualTo(true);
        assertThat(domain
                .getValues()
                .getRanges()
                .getOrderedRanges()
                .getFirst()
                .getSingleValue()).isEqualTo(1L);
    }

    @Test
    void testMultiPartitionValue()
            throws VastException
    {
        ConnectorSession connectorSession = prepareSession();
        VastClient vastClient = prepareClient();
        VastTransaction vastTransaction = prepareTx();
        Field fieldX = new Field("x",
                new FieldType(true, new ArrowType.Utf8(), null, metadata),
                null);
        Field fieldY = new Field("y",
                new FieldType(true, new ArrowType.Utf8(), null, metadata),
                null);
        Field fieldZ = new Field("z",
                new FieldType(true, new ArrowType.Utf8(), null, metadata),
                null);
        when(vastClient.listColumns(any(), any(), any(), anyInt(), any(),
                any())).thenReturn(new Schema(List.of(fieldX, fieldY, fieldZ)));
        VastColumnHandle colHandleX = VastColumnHandle.fromField(fieldX);
        VastColumnHandle colHandleY = VastColumnHandle.fromField(fieldY);
        VastColumnHandle colHandleZ = VastColumnHandle.fromField(fieldZ);
        VastTableHandle tableHandle = prepareTableHandle(List.of("x", "y"),
                colHandleX, colHandleY, colHandleZ);
        VastPageSourceProvider vastPageSourceProvider = mock(
                VastPageSourceProvider.class);
        ConnectorPageSource pageSource = mock(ConnectorPageSource.class);
        when(pageSource.isFinished()).thenReturn(false, true);
        VariableWidthBlockBuilder blockBuilderX = VarcharType.createVarcharType(
                8).createBlockBuilder(null, 1);
        blockBuilderX.writeEntry(Slices.utf8Slice("Xpart1"));
        VariableWidthBlockBuilder blockBuilderY = VarcharType.createVarcharType(
                8).createBlockBuilder(null, 1);
        blockBuilderY.writeEntry(Slices.utf8Slice("Ypart1"));
        LongArrayBlockBuilder rowCountBlockBuilder = createRowEstimateBlock(
                100);
        Page page = new Page(blockBuilderX.build(), blockBuilderY.build(),
                rowCountBlockBuilder.build());
        when(pageSource.getNextSourcePage()).thenReturn(
                SourcePage.create(page));
        when(vastPageSourceProvider.createPageSource(any(), any(), any(), any(),
                any(), any())).thenReturn(pageSource);
        VastPartitionedSplitSource splitSource = new VastPartitionedSplitSource(
                nodeManager, vastClient, statisticsManager,
                vastPageSourceProvider, vastTransactionHandleManager,
                splitSourceMetrics, vastTransaction, connectorSession,
                tableHandle, DynamicFilter.EMPTY);
        List<ConnectorSplit> splits = splitSource
                .getNextBatch(100)
                .join()
                .getSplits();
        assertThat(splits.size()).isEqualTo(1);
        assertThat(splits.getFirst()).isInstanceOf(VastSplit.class);
        VastSplit vastSplit = (VastSplit) splits.getFirst();
        TupleDomain<VastColumnHandle> partitionFilters = vastSplit.getFilters();
        assertThat(partitionFilters.getDomains().isPresent()).isTrue();
        Map<VastColumnHandle, Domain> domains = partitionFilters
                .getDomains()
                .orElseThrow();
        assertThat(domains.size()).isEqualTo(2);
        List<Object> partitionColHandle = domains.values().stream().map(
                Domain::getSingleValue).toList();
        assertThat(partitionColHandle).containsAll(
                List.of(Slices.utf8Slice("Xpart1"),
                        Slices.utf8Slice("Ypart1")));
    }

    @Test
    void testMultiPartitionTypes()
            throws VastException
    {
        ConnectorSession connectorSession = prepareSession();
        VastClient vastClient = prepareClient();
        VastTransaction vastTransaction = prepareTx();
        Field fieldX = new Field("x",
                new FieldType(true, new ArrowType.Utf8(), null, metadata),
                null);
        Field fieldY = new Field("y",
                new FieldType(true, new ArrowType.Int(32, true), null,
                        metadata), null);
        Field fieldZ = new Field("z",
                new FieldType(true, new ArrowType.Utf8(), null, metadata),
                null);

        when(vastClient.listColumns(any(), any(), any(), anyInt(), any(),
                any())).thenReturn(new Schema(List.of(fieldX, fieldY, fieldZ)));
        VastColumnHandle colHandleX = VastColumnHandle.fromField(fieldX);
        VastColumnHandle colHandleY = VastColumnHandle.fromField(fieldY);
        VastColumnHandle colHandleZ = VastColumnHandle.fromField(fieldZ);
        VastTableHandle tableHandle = prepareTableHandle(List.of("x", "y"),
                colHandleX, colHandleY, colHandleZ);
        VastPageSourceProvider vastPageSourceProvider = mock(
                VastPageSourceProvider.class);
        ConnectorPageSource pageSource = mock(ConnectorPageSource.class);
        when(pageSource.isFinished()).thenReturn(false, true);
        VariableWidthBlockBuilder blockBuilderX = VarcharType.createVarcharType(
                8).createBlockBuilder(null, 1);
        blockBuilderX.writeEntry(Slices.utf8Slice("Xpart1"));
        IntArrayBlockBuilder blockBuilderY = (IntArrayBlockBuilder) INTEGER.createBlockBuilder(
                null, 1);
        blockBuilderY.writeInt(6);
        LongArrayBlockBuilder rowCountBlockBuilder = createRowEstimateBlock(
                100);
        Page page = new Page(blockBuilderX.build(), blockBuilderY.build(),
                rowCountBlockBuilder.build());
        when(pageSource.getNextSourcePage()).thenReturn(
                SourcePage.create(page));
        when(vastPageSourceProvider.createPageSource(any(), any(), any(), any(),
                any(), any())).thenReturn(pageSource);
        VastPartitionedSplitSource splitSource = new VastPartitionedSplitSource(
                nodeManager, vastClient, statisticsManager,
                vastPageSourceProvider, vastTransactionHandleManager,
                splitSourceMetrics, vastTransaction, connectorSession,
                tableHandle, DynamicFilter.EMPTY);
        List<ConnectorSplit> splits = splitSource
                .getNextBatch(100)
                .join()
                .getSplits();
        assertThat(splits.size()).isEqualTo(1);
        assertThat(splits.getFirst()).isInstanceOf(VastSplit.class);
        VastSplit vastSplit = (VastSplit) splits.getFirst();
        TupleDomain<VastColumnHandle> partitionFilters = vastSplit.getFilters();
        assertThat(partitionFilters.getDomains().isPresent()).isTrue();
        Map<VastColumnHandle, Domain> domains = partitionFilters
                .getDomains()
                .orElseThrow();
        assertThat(domains.size()).isEqualTo(2);
        List<Object> partitionColHandle = domains.values().stream().map(
                Domain::getSingleValue).toList();
        assertThat(partitionColHandle).contains(Slices.utf8Slice("Xpart1"));
        assertThat(partitionColHandle).contains(6L);
    }

    @Test
    void testDynamicFilterFilterOutAllPartitions()
            throws VastException
    {
        ConnectorSession connectorSession = prepareSession();
        VastClient vastClient = prepareClient();
        VastTransaction vastTransaction = prepareTx();
        Field fieldX = new Field("x",
                new FieldType(true, new ArrowType.Date(DateUnit.DAY), null,
                        metadata), null);
        when(vastClient.listColumns(any(), any(), any(), anyInt(), any(),
                any())).thenReturn(new Schema(List.of(fieldX)));
        VastColumnHandle colHandle = VastColumnHandle.fromField(fieldX);
        VastTableHandle tableHandle = prepareTableHandle(List.of("x"),
                colHandle);
        VastPageSourceProvider vastPageSourceProvider = mock(
                VastPageSourceProvider.class);
        ConnectorPageSource pageSource = mock(ConnectorPageSource.class);
        when(pageSource.isFinished()).thenReturn(false, true);
        IntArrayBlockBuilder blockBuilder = (IntArrayBlockBuilder) DateType.DATE.createBlockBuilder(
                null, 1);
        blockBuilder.writeInt(1);
        LongArrayBlockBuilder rowCountBlockBuilder = createRowEstimateBlock(
                200);
        Page page = new Page(blockBuilder.build(),
                rowCountBlockBuilder.build());
        when(pageSource.getNextSourcePage()).thenReturn(
                SourcePage.create(page));
        when(vastPageSourceProvider.createPageSource(any(), any(), any(), any(),
                any(), any())).thenReturn(pageSource);
        DynamicFilter dynamicFilter = mock(DynamicFilter.class);
        when(dynamicFilter.getCurrentPredicate()).thenReturn(
                TupleDomain.withColumnDomains(Map.of(colHandle,
                        Domain.singleValue(DateType.DATE,
                                2L)))); // different date should filter out all partitions
        VastPartitionedSplitSource splitSource = new VastPartitionedSplitSource(
                nodeManager, vastClient, statisticsManager,
                vastPageSourceProvider, vastTransactionHandleManager,
                splitSourceMetrics, vastTransaction, connectorSession,
                tableHandle, dynamicFilter);
        List<ConnectorSplit> splits = splitSource
                .getNextBatch(100)
                .join()
                .getSplits();
        assertThat(splits.size()).isEqualTo(0);
    }

    @Test
    void testDynamicFilterDifferentColumn()
            throws VastException
    {
        ConnectorSession connectorSession = prepareSession();
        VastClient vastClient = prepareClient();
        VastTransaction vastTransaction = prepareTx();
        Field fieldX = new Field("x",
                new FieldType(true, new ArrowType.Int(32, true), null,
                        metadata), null);
        Field fieldY = new Field("y",
                new FieldType(true, new ArrowType.Int(32, true), null,
                        metadata), null);
        when(vastClient.listColumns(any(), any(), any(), anyInt(), any(),
                any())).thenReturn(new Schema(List.of(fieldX)));
        VastColumnHandle colHandleX = VastColumnHandle.fromField(fieldX);
        VastColumnHandle colHandleY = VastColumnHandle.fromField(fieldY);
        VastTableHandle tableHandle = prepareTableHandle(List.of("x"),
                colHandleX, colHandleY);
        VastPageSourceProvider vastPageSourceProvider = mock(
                VastPageSourceProvider.class);
        ConnectorPageSource pageSource = mock(ConnectorPageSource.class);
        when(pageSource.isFinished()).thenReturn(false, true);
        IntArrayBlockBuilder blockBuilder = (IntArrayBlockBuilder) INTEGER.createBlockBuilder(
                null, 1);
        blockBuilder.writeInt(1);
        LongArrayBlockBuilder rowCountBlockBuilder = createRowEstimateBlock(
                200);
        Page page = new Page(blockBuilder.build(),
                rowCountBlockBuilder.build());
        when(pageSource.getNextSourcePage()).thenReturn(
                SourcePage.create(page));
        when(vastPageSourceProvider.createPageSource(any(), any(), any(), any(),
                any(), any())).thenReturn(pageSource);
        DynamicFilter dynamicFilter = mock(DynamicFilter.class);
        when(dynamicFilter.getCurrentPredicate()).thenReturn(
                TupleDomain.withColumnDomains(Map.of(colHandleY,
                        Domain.singleValue(INTEGER,
                                1L)))); // different date should filter out all partitions
        VastPartitionedSplitSource splitSource = new VastPartitionedSplitSource(
                nodeManager, vastClient, statisticsManager,
                vastPageSourceProvider, vastTransactionHandleManager,
                splitSourceMetrics, vastTransaction, connectorSession,
                tableHandle, dynamicFilter);
        List<ConnectorSplit> splits = splitSource
                .getNextBatch(100)
                .join()
                .getSplits();
        assertThat(splits.size()).isEqualTo(1);
        VastSplit split = (VastSplit) splits.getFirst();
        TupleDomain<VastColumnHandle> filters = split.getFilters();
        Map<VastColumnHandle, Domain> domains = filters
                .getDomains()
                .orElseThrow();
        assertThat(domains.size()).isEqualTo(2);
        assertThat(domains.keySet()).containsAll(
                List.of(colHandleX, colHandleY));
    }

    @Test
    void testDynamicFilterPushdownToPIT()
            throws VastException
    {
        ConnectorSession connectorSession = prepareSession();
        VastClient vastClient = prepareClient();
        VastTransaction vastTransaction = prepareTx();
        Field fieldX = new Field("x",
                new FieldType(true, new ArrowType.Int(32, true), null,
                        metadata), null);
        Field fieldY = new Field("y",
                new FieldType(true, new ArrowType.Int(32, true), null,
                        metadata), null);
        when(vastClient.listColumns(any(), any(), any(), anyInt(), any(),
                any())).thenReturn(new Schema(List.of(fieldX)));
        VastColumnHandle colHandleX = VastColumnHandle.fromField(fieldX);
        VastColumnHandle colHandleY = VastColumnHandle.fromField(fieldY);
        VastTableHandle tableHandle = prepareTableHandle(List.of("x"),
                colHandleX, colHandleY);
        VastPageSourceProvider vastPageSourceProvider = mock(
                VastPageSourceProvider.class);
        ConnectorPageSource pageSource = mock(ConnectorPageSource.class);
        when(pageSource.isFinished()).thenReturn(false, true);
        IntArrayBlockBuilder blockBuilder = (IntArrayBlockBuilder) INTEGER.createBlockBuilder(
                null, 1);
        blockBuilder.writeInt(1);
        LongArrayBlockBuilder rowCountBlockBuilder = createRowEstimateBlock(
                200);
        Page page = new Page(blockBuilder.build(),
                rowCountBlockBuilder.build());
        when(pageSource.getNextSourcePage()).thenReturn(
                SourcePage.create(page));
        ArgumentCaptor<VastSplit> splitCaptor = ArgumentCaptor.forClass(
                VastSplit.class);
        when(vastPageSourceProvider.createPageSource(any(), any(),
                splitCaptor.capture(), any(), any(), any())).thenReturn(
                pageSource);
        DynamicFilter dynamicFilter = mock(DynamicFilter.class);
        when(dynamicFilter.getCurrentPredicate()).thenReturn(
                TupleDomain.withColumnDomains(Map.of(colHandleX,
                        Domain.singleValue(INTEGER,
                                1L)))); // different date should filter out all partitions
        VastPartitionedSplitSource splitSource = new VastPartitionedSplitSource(
                nodeManager, vastClient, statisticsManager,
                vastPageSourceProvider, vastTransactionHandleManager,
                splitSourceMetrics, vastTransaction, connectorSession,
                tableHandle, dynamicFilter);
        splitSource.getNextBatch(100).join();
        verify(vastPageSourceProvider).createPageSource(any(), any(),
                splitCaptor.capture(), any(), any(), any());
        VastSplit pitSplit = splitCaptor.getValue();

        assertThat(pitSplit
                .getFilters()
                .getDomain(colHandleX, INTEGER)
                .getValues()
                .isSingleValue()).isTrue();
    }

    @Test
    void testDynamicFilterPushdownToPITForNonIdentityTransforms()
            throws VastException
    {
        ConnectorSession connectorSession = prepareSession();
        VastClient vastClient = prepareClient();
        VastTransaction vastTransaction = prepareTx();
        FieldType arrowIntegerFieldType = new FieldType(true, new ArrowType.Int(32, true), null, null);

        List<Field> tableFields = List.of(
                new Field("b", arrowIntegerFieldType, null),
                new Field("y", arrowIntegerFieldType, null),
                new Field("m", arrowIntegerFieldType, null),
                new Field("h", arrowIntegerFieldType, null),
                new Field("t", arrowIntegerFieldType, null));

        List<Field> pitFields = List.of(
                new Field("b_bucket", arrowIntegerFieldType, null),
                new Field("y_year", arrowIntegerFieldType, null),
                new Field("m_month", arrowIntegerFieldType, null),
                new Field("h_hour", arrowIntegerFieldType, null),
                new Field("t_trunc", arrowIntegerFieldType, null));

        when(vastClient.listColumns(any(), any(), any(), anyInt(), any(), any())).thenReturn(new Schema(pitFields));
        List<VastColumnHandle> columnHandles = tableFields.stream().map(VastColumnHandle::fromField).toList();

        VastTableHandle tableHandle = new VastTableHandle("buck/schem", "tab", createObjectDetails("tab", "id"), false, false)
                .withPartitionColumns(List.of(
                        new PartitionColumnMetadata("b_bucket", "int32", "b", "int32", "Bucket", 16),
                        new PartitionColumnMetadata("y_year", "int32", "y", "int32", "Year", null),
                        new PartitionColumnMetadata("m_month", "int32", "m", "int32", "Month", null),
                        new PartitionColumnMetadata("h_hour", "int32", "h", "int32", "Hour", null),
                        new PartitionColumnMetadata("t_trunc", "int32", "t", "int32", "Truncate", 3)));
        tableHandle.setColumnHandlesCache(columnHandles);

        VastPageSourceProvider vastPageSourceProvider = mock(VastPageSourceProvider.class);
        ArgumentCaptor<VastSplit> splitCaptor = ArgumentCaptor.forClass(VastSplit.class);
        when(vastPageSourceProvider.createPageSource(any(), any(), splitCaptor.capture(), any(), any(), any())).thenReturn(new EmptyPageSource());

        DynamicFilter dynamicFilter = mock(DynamicFilter.class);
        when(dynamicFilter.getCurrentPredicate()).thenReturn(
                TupleDomain.withColumnDomains(
                        IntStream.range(0, columnHandles.size())
                                .boxed()
                                .collect(Collectors.toMap(
                                        columnHandles::get,
                                        i -> Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 42L)), true)))));

        VastPartitionedSplitSource splitSource = new VastPartitionedSplitSource(nodeManager,
                vastClient,
                statisticsManager,
                vastPageSourceProvider,
                vastTransactionHandleManager,
                splitSourceMetrics,
                vastTransaction,
                connectorSession,
                tableHandle,
                dynamicFilter);
        splitSource.getNextBatch(100).join();

        VastSplit pitSplit = splitCaptor.getValue();
        Map<VastColumnHandle, Domain> domainsPushedToPit = pitSplit.getFilters().getDomains().orElseThrow();

        List<String> domainNames = domainsPushedToPit.keySet()
                .stream()
                .map(columnHandle -> columnHandle.getField().getName())
                .toList();

        assertThat(domainNames).containsExactlyInAnyOrder("b", "y", "m", "h", "t");
        domainsPushedToPit.values().forEach(domain -> {
            Ranges ranges = domain.getValues().getRanges();
            assertThat(ranges.getRangeCount()).isEqualTo(1);
            assertThat(ranges.getOrderedRanges().getFirst().getLowBoundedValue()).isEqualTo(42L);
            assertThat(ranges.getOrderedRanges().getFirst().getHighValue()).isEmpty();
        });
    }
}
