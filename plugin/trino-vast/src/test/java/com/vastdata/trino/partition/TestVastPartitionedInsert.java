/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.partition;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.vastdata.TableLayout;
import com.vastdata.client.VastClient;
import com.vastdata.trino.VastConnectorFactory;
import com.vastdata.trino.VastModule;
import com.vastdata.trino.VastPageSinkProvider;
import com.vastdata.trino.partition.PartitionKeyHashFunction.IndexBase;
import com.vastdata.trino.tx.VastTransactionHandle;
import com.vastdata.trino.tx.VastTrinoTransactionHandleManager;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.spi.Page;
import io.trino.spi.Plugin;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.DateType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.vastdata.client.schema.TestVastMetadataUtils.createObjectDetails;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestVastPartitionedInsert
{
    private static final Logger LOG = Logger.get(
            TestVastPartitionedInsert.class);

    // This test doesn't make sense if there are less then two sinks and if
    // at least two sinks don't get data
    private static final int MIN_NUM_OF_SINKS = 2;
    private static final String SCHEMA_NAME = "testschema";
    private static final String TABLE_NAME = "testtable";
    private TypeOperators typeOperators;
    private QueryRunner queryRunner;
    private VastPageSinkProvider mockPageSinkProvider;
    private VastClient mockClient;

    public static Page createIntPage(List<List<Integer>> rows)
    {
        if (rows.isEmpty()) {
            return new Page(0);
        }

        int columnCount = rows.get(0).size();
        int rowCount = rows.size();

        BlockBuilder[] columnBuilders = IntStream
                .range(0, columnCount)
                .mapToObj(i -> IntegerType.INTEGER.createBlockBuilder(null,
                        rowCount))
                .toArray(BlockBuilder[]::new);

        for (List<Integer> row : rows) {
            for (int i = 0; i < columnCount; i++) {
                Integer value = row.get(i);
                if (value == null) {
                    columnBuilders[i].appendNull();
                }
                else {
                    IntegerType.INTEGER.writeInt(columnBuilders[i], value);
                }
            }
        }

        Block[] blocks = new Block[columnCount];
        for (int i = 0; i < columnCount; i++) {
            blocks[i] = columnBuilders[i].build();
        }

        return new Page(rowCount, blocks);
    }

    /**
     * Provides test arguments for various partition column configurations.
     *
     * returns 2 - 5 columns. always 1 regular (id column) and the rest
     * partitioned with identity 3 variations for each of the options: - id is
     * the first column - id is the last - id is the second
     */
    private static Stream<Arguments> partitionProvider()
    {
        ColumnDef idColumn = new ColumnDef("id", Optional.empty());
        IntFunction<List<ColumnDef>> partitioner = numFunctions -> IntStream
                .rangeClosed(1, numFunctions)
                .mapToObj(i -> new ColumnDef("part_key" + i, Optional.of(
                        new PartitionDef("identity", OptionalInt.empty(),
                                IntegerType.INTEGER))))
                .toList();

        return IntStream
                .rangeClosed(1, 4)
                .mapToObj(partitioner)
                .flatMap(partitionCols ->
                {
                    // id first
                    List<ColumnDef> idFirst = new ArrayList<>();
                    idFirst.add(idColumn);
                    idFirst.addAll(partitionCols);

                    // id last
                    List<ColumnDef> idLast = new ArrayList<>(partitionCols);
                    idLast.add(idColumn);

                    // id in second place
                    List<ColumnDef> idInSecondPlace = new ArrayList<>();
                    idInSecondPlace.add(partitionCols.get(0));
                    idInSecondPlace.add(idColumn);
                    idInSecondPlace.addAll(
                            partitionCols.subList(1, partitionCols.size()));

                    return Stream.of(idFirst, idLast, idInSecondPlace);
                })
                .map(Arguments::of);
    }

    private static Stream<Arguments> transformSanityProvider()
    {
        return Stream.of(
                Arguments.of("identity on integer",
                        new ColumnDef("part_key",
                                Optional.of(new PartitionDef("identity", OptionalInt.empty(), IntegerType.INTEGER))),
                        (IntFunction<String>) String::valueOf, 16),
                Arguments.of("year on date", new ColumnDef("part_key",
                                Optional.of(new PartitionDef("year", OptionalInt.empty(),
                                        DateType.DATE))),
                        (IntFunction<String>) i -> "DATE '20" + String.format("%02d", i) + "-01-01'", 16),
                Arguments.of("month on date", new ColumnDef("part_key",
                                Optional.of(new PartitionDef("month", OptionalInt.empty(), DateType.DATE))),
                        (IntFunction<String>) i -> "DATE '2023-" + String.format("%02d", i % 12 + 1) + "-01'", 12),
                Arguments.of("day on date",
                        new ColumnDef("part_key", Optional.of(
                                new PartitionDef("day", OptionalInt.empty(),
                                        DateType.DATE))),
                        (IntFunction<String>) i -> "DATE '2023-01-" + String.format(
                                "%02d", i + 1) + "'", 16),
                Arguments.of("hour on timestamp", new ColumnDef("part_key",
                                Optional.of(new PartitionDef("hour", OptionalInt.empty(),
                                        TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS))),
                        (IntFunction<String>) i -> "TIMESTAMP '2023-01-01 " + String.format("%02d", i) + ":00:00.000000 UTC'", 16),
                Arguments.of("bucket on int", new ColumnDef("part_key",
                                Optional.of(new PartitionDef("bucket", OptionalInt.of(4), IntegerType.INTEGER))), (IntFunction<String>) String::valueOf, 4),
                Arguments.of("truncate on string", new ColumnDef("part_key",
                                Optional.of(new PartitionDef("truncate", OptionalInt.of(2), VarcharType.VARCHAR))),
                        (IntFunction<String>) i -> "'" + (char) ('a' + i) + (char) ('a' + i) + "foo'", 16));
    }

    @BeforeAll
    public void setupQueryRunner()
            throws Exception
    {
        typeOperators = new TypeOperators();
        mockPageSinkProvider = mock(VastPageSinkProvider.class);
        mockClient = mock(VastClient.class);

        resetMocks();

        Session session = testSessionBuilder()
                .setCatalog("vast")
                .setSchema(SCHEMA_NAME)
                .build();

        queryRunner = DistributedQueryRunner
                .builder(session)
                .setWorkerCount(2)
                .build();
        queryRunner.installPlugin(
                new TestingVastPlugin(mockPageSinkProvider, mockClient));
        queryRunner.createCatalog("vast", "vast",
                ImmutableMap.of("node.environment", "testing"));
    }

    @AfterAll
    public void teardownQueryRunner()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    private void resetMocks()
    {
        reset(mockPageSinkProvider);
        reset(mockClient);

        try {
            when(mockClient.listAllSchemas(any(), anyInt(),
                    anyString())).thenReturn(
                    ImmutableList.of(SCHEMA_NAME).stream());

            when(mockClient.getVastTableHandleId(any(), eq(SCHEMA_NAME),
                    eq(TABLE_NAME), anyString()))
                    .thenReturn(Optional.empty())
                    .thenReturn(Optional.of(createObjectDetails(TABLE_NAME, "id")));
            when(mockClient.fetchTableLayout(any(), anyString(), anyString(),
                    anyInt(), any(), anyString())).thenReturn(
                    TableLayout.EMPTY);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<VastPartitionFunction> columnDefsToPartitionFunctions(List<ColumnDef> columnDefs)
    {
        Map<String, Integer> columnNameToIndex = IntStream
                .range(0, columnDefs.size())
                .boxed()
                .collect(Collectors.toMap(i -> columnDefs.get(i).columnName(),
                        i -> i));

        List<ColumnDef> partitionedColumnDefs = columnDefs
                .stream()
                .filter(ColumnDef::isPartitionColumn)
                .toList();

        return IntStream
                .range(0, partitionedColumnDefs.size())
                .mapToObj(partitionIdx ->
                {
                    ColumnDef partitionColDef = partitionedColumnDefs.get(
                            partitionIdx);
                    Integer columnIdx = columnNameToIndex.get(
                            partitionColDef.columnName);
                    return partitionColDef.toPartitionFunction(partitionIdx,
                            columnIdx);
                })
                .flatMap(Optional::stream)
                .toList();
    }

    public TestSetup setupForTest(List<ColumnDef> columnDefs)
    {
        resetMocks();
        List<VastPartitionFunction> partitionFunctions = columnDefsToPartitionFunctions(
                columnDefs);

        AtomicInteger pageSinkId = new AtomicInteger(1);
        when(mockPageSinkProvider.createPageSink(
                any(ConnectorTransactionHandle.class),
                any(ConnectorSession.class),
                any(ConnectorOutputTableHandle.class),
                any(ConnectorPageSinkId.class))).thenAnswer(
                        _ -> new MockVastPageSink(pageSinkId.getAndIncrement(),
                                PartitionKeyHashFunction.create(partitionFunctions,
                                        typeOperators, IndexBase.BY_COLUMN_INDEX)));

        MockVastPageSink.PARTITION_DATA.clear();

        PartitionKeyHashFunction hashFunction = PartitionKeyHashFunction.create(
                partitionFunctions, typeOperators,
                IndexBase.BY_PARTITION_INDEX);

        List<ColumnDef> partitionedColumnDefs = columnDefs
                .stream()
                .filter(ColumnDef::isPartitionColumn)
                .toList();
        int nRepeatingPartitionedColumnsValues = 73;

        // to keep the total number of partitions generated by cartesian product at a reasonable level for the test
        int valuesPerPart = 4 * (6 - columnDefs.size());

        List<Integer> range = IntStream
                .range(0, valuesPerPart)
                .boxed()
                .toList();
        List<List<Integer>> lists = Collections.nCopies(
                partitionedColumnDefs.size(), range);
        Page partitionColumnsValues = createIntPage(
                Lists.cartesianProduct(lists));
        List<Long> partitionKeyHashes = IntStream
                .range(0, partitionColumnsValues.getPositionCount())
                .mapToObj(
                        pos -> hashFunction.apply(partitionColumnsValues, pos))
                .toList();

        StringJoiner sqlValues = new StringJoiner(", ");
        int nRows = 0;
        for (int pos = 0; pos < partitionColumnsValues.getPositionCount(); pos++) {
            for (int j = 0; j < nRepeatingPartitionedColumnsValues; j++) {
                StringJoiner rowValues = new StringJoiner(", ");
                int partIdx = 0;

                for (ColumnDef def : columnDefs) {
                    if (def.partitionDef().isPresent()) {
                        Block b = partitionColumnsValues.getBlock(partIdx);
                        long v = IntegerType.INTEGER.getInt(b, pos);
                        rowValues.add(String.valueOf(v));
                        partIdx++;
                    }
                    else {
                        rowValues.add(String.valueOf(nRows));
                    }
                }
                sqlValues.add(String.format("(%s)", rowValues));
                nRows++;
            }
        }

        String partitioningClause = getPartitioningClause(columnDefs);

        String allColumnNamesForSelect = columnDefs
                .stream()
                .map(ColumnDef::columnName)
                .map(name -> "t." + name)
                .collect(java.util.stream.Collectors.joining(", "));
        String allColumnNamesForValues = String.join(", ",
                columnDefs.stream().map(ColumnDef::columnName).toList());

        String sqlInsert = getDistributedInsertQuery(sqlValues.toString(),
                partitioningClause, allColumnNamesForSelect,
                allColumnNamesForValues);

        return new TestSetup(sqlInsert, partitionKeyHashes,
                nRepeatingPartitionedColumnsValues, nRows);
    }

    private String getDistributedInsertQuery(String sqlValues,
                                             String partitioningClause,
                                             String projectionClause,
                                             String insertValuesClause)
    {
        // The `CROSS JOIN` is a trick to make trino distribute the query and not run it on a single node / sink
        // it is undone by projection and distinct

        String sqlInsert = String.format(
                "CREATE TABLE %s WITH (partitioning = ARRAY[%s]) AS SELECT DISTINCT %s FROM (VALUES %s) AS t(%s) CROSS JOIN system.runtime.nodes",
                TABLE_NAME, partitioningClause, projectionClause, sqlValues,
                insertValuesClause);
        return sqlInsert;
    }

    public String getPartitioningClause(List<ColumnDef> columnDefs)
    {
        return columnDefs
                .stream()
                .filter(ColumnDef::isPartitionColumn)
                .map(column ->
                {
                    PartitionDef partitionDef = column.partitionDef.orElseThrow();
                    if (partitionDef.transformArg.isPresent()) {
                        return String.format("'%s(%s, %s)'",
                                partitionDef.transform, column.columnName(),
                                partitionDef.transformArg.orElseThrow());
                    }
                    return String.format("'%s(%s)'", partitionDef.transform,
                            column.columnName());
                })
                .collect(Collectors.joining(", "));
    }

    @ParameterizedTest(name = "partitionKeys = {0}")
    @MethodSource("partitionProvider")
    public void testPartitionedCtas(List<ColumnDef> expectedCols)
    {
        TestSetup setup = setupForTest(expectedCols);

        assertUpdate(setup.sqlInsert, setup.totalExpectedRows);

        verify(mockPageSinkProvider, atLeast(MIN_NUM_OF_SINKS)).createPageSink(
                any(ConnectorTransactionHandle.class),
                any(ConnectorSession.class),
                any(ConnectorOutputTableHandle.class),
                any(ConnectorPageSinkId.class));
        assertTrue(MockVastPageSink.PARTITION_DATA
                        .keySet()
                        .stream()
                        .map(MockVastPageSink.SinkAndPartition::sinkId)
                        .collect(Collectors.toSet())
                        .size() >= MIN_NUM_OF_SINKS,
                "we want at least two sinks to get data for this test to make sense");
        // Assertions
        // 1. We expect two distinct partitions to be created.
        assertEquals(setup.partitionKeyHashes().size(),
                MockVastPageSink.PARTITION_DATA.size(),
                "number of sink-partition tuples should be the same as number of partitions, because each partition should arrive to exactly one sink");

        assertEquals(setup.totalExpectedRows, MockVastPageSink.PARTITION_DATA
                        .values()
                        .stream()
                        .reduce(0, Integer::sum),
                "number of total rows should not as expected");

        for (int count : MockVastPageSink.PARTITION_DATA.values()) {
            assertEquals(setup.nRepeatingPartitionedColumnsValues, count,
                    "each partition should have these many rows because these are identity transforms.");
        }
        Set<Long> actualPartitionIds = MockVastPageSink.PARTITION_DATA
                .keySet()
                .stream()
                .map(MockVastPageSink.SinkAndPartition::partitionKeyHash)
                .collect(Collectors.toSet());
        Set<Long> expectedPartitionIds = setup.partitionKeyHashes
                .stream()
                .collect(Collectors.toSet());
        assertEquals(expectedPartitionIds, actualPartitionIds,
                "all partition IDs from 1 to num of partitions should be present");
    }

    private TestSetup2 setupForTransformTest(List<ColumnDef> columnDefs,
                                             IntFunction<String> partitionValueGenerator)
    {
        resetMocks();
        List<VastPartitionFunction> partitionFunctions = columnDefsToPartitionFunctions(
                columnDefs);

        AtomicInteger pageSinkId = new AtomicInteger(1);
        when(mockPageSinkProvider.createPageSink(any(ConnectorTransactionHandle.class),
                any(ConnectorSession.class),
                any(ConnectorOutputTableHandle.class),
                any(ConnectorPageSinkId.class)))
                .thenAnswer(_ -> new MockVastPageSink(pageSinkId.getAndIncrement(),
                PartitionKeyHashFunction.create(partitionFunctions, typeOperators, IndexBase.BY_COLUMN_INDEX)));

        MockVastPageSink.PARTITION_DATA.clear();

        final int nRowsPerValue = 73;
        StringJoiner sqlValues = new StringJoiner(", ");
        AtomicInteger id = new AtomicInteger(1);

        final int numDistinctPartitionValues = 16;

        IntStream.range(0, numDistinctPartitionValues).forEach(i ->
        {
            String partitionValue = partitionValueGenerator.apply(i);
            IntStream.range(0, nRowsPerValue).forEach(row ->
            {
                StringJoiner rowValues = new StringJoiner(", ");
                for (ColumnDef def : columnDefs) {
                    if (def.partitionDef().isPresent()) {
                        rowValues.add(partitionValue);
                    }
                    else {
                        rowValues.add(String.valueOf(id.getAndIncrement()));
                    }
                }
                sqlValues.add(String.format("(%s)", rowValues));
            });
        });
        String partitioningClause = getPartitioningClause(columnDefs);

        String allColumnNamesForSelect = columnDefs
                .stream()
                .map(ColumnDef::columnName)
                .map(name -> "t." + name)
                .collect(Collectors.joining(", "));
        String allColumnNamesForValues = String.join(", ",
                columnDefs.stream().map(ColumnDef::columnName).toList());

        String sqlInsert = getDistributedInsertQuery(sqlValues.toString(),
                partitioningClause, allColumnNamesForSelect,
                allColumnNamesForValues);

        return new TestSetup2(sqlInsert,
                numDistinctPartitionValues * nRowsPerValue);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("transformSanityProvider")
    public void testPartitionedCtasWithTransforms(String name,
                                                  ColumnDef partitionColumn,
                                                  IntFunction<String> valueGenerator,
                                                  int numExpectedPartitions)
    {
        List<ColumnDef> columnDefs = ImmutableList.of(
                new ColumnDef("id", Optional.empty()), partitionColumn);

        TestSetup2 setup = setupForTransformTest(columnDefs, valueGenerator);

        assertUpdate(setup.sqlInsert, setup.totalExpectedRows);

        if (numExpectedPartitions > 1) {
            int atLeastNumOfSinks = 2;
            verify(mockPageSinkProvider,
                    atLeast(atLeastNumOfSinks)).createPageSink(
                    any(ConnectorTransactionHandle.class),
                    any(ConnectorSession.class),
                    any(ConnectorOutputTableHandle.class),
                    any(ConnectorPageSinkId.class));
            assertTrue(MockVastPageSink.PARTITION_DATA
                            .keySet()
                            .stream()
                            .map(MockVastPageSink.SinkAndPartition::sinkId)
                            .collect(Collectors.toSet())
                            .size() >= atLeastNumOfSinks,
                    "we want at least two sinks to get data for this test to make sense");
        }

        Set<Long> distinctPartitions = MockVastPageSink.PARTITION_DATA
                .keySet()
                .stream()
                .map(MockVastPageSink.SinkAndPartition::partitionKeyHash)
                .collect(Collectors.toSet());
        assertEquals(numExpectedPartitions, distinctPartitions.size(),
                "number of distinct partitions should be as expected for " + name);

        long totalRowsInSinks = MockVastPageSink.PARTITION_DATA
                .values()
                .stream()
                .mapToLong(Integer::longValue)
                .sum();
        assertEquals(setup.totalExpectedRows, totalRowsInSinks,
                "total number of rows in sinks should be correct");
    }

    // Helper to run the query and assert the number of rows affected
    private void assertUpdate(String sql, long count)
    {
        long updatedRows = (long) queryRunner.execute(sql).getOnlyValue();
        assertEquals(count, updatedRows,
                "Expected " + count + " rows to be updated, but got " + updatedRows);
    }

    // A testing plugin that uses our pre-configured mocks
    private record TestingVastPlugin(VastPageSinkProvider pageSinkProvider,
            VastClient mockClient)
            implements Plugin
    {
        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            VastTrinoTransactionHandleManager mockTM = mock(
                    VastTrinoTransactionHandleManager.class);
            when(mockTM.isOpen(any(VastTransactionHandle.class))).thenReturn(
                    true);
            when(mockTM.startTransaction(any())).thenReturn(
                    new VastTransactionHandle(0));

            VastModule vastModule = VastModule
                    .builder(false)
                    .withPageSinkProvider(pageSinkProvider)
                    .withVastClient(mockClient)
                    .withTransactionManager(mockTM)
                    .build();
            return List.of(new VastConnectorFactory(vastModule));
        }
    }

    public record TestSetup(String sqlInsert,
            List<Long> partitionKeyHashes,
            int nRepeatingPartitionedColumnsValues,
            int totalExpectedRows)
    {}

    public record PartitionDef(String transform,
            OptionalInt transformArg,
            Type type)
    {}

    public record ColumnDef(String columnName,
            Optional<PartitionDef> partitionDef)
    {
        public Optional<VastPartitionFunction> toPartitionFunction(Integer partitionIdx,
                                                                   Integer columnIdx)
        {
            return partitionDef.map(
                    pd -> VastPartitionFunction.create(pd.transform,
                            pd.transformArg, pd.type, columnName, partitionIdx,
                            columnIdx));
        }

        public boolean isPartitionColumn()
        {
            return partitionDef.isPresent();
        }
    }

    public record TestSetup2(String sqlInsert,
            int totalExpectedRows)
    {}
}
