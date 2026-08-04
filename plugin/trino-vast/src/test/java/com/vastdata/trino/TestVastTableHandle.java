/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.VastObjectDetails;
import com.vastdata.client.partition.PartitionColumnMetadata;
import io.airlift.json.JsonCodec;
import io.airlift.testing.EquivalenceTester;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.vastdata.client.schema.TestVastMetadataUtils.createObjectDetails;
import static io.airlift.json.JsonCodec.jsonCodec;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestVastTableHandle
{
    private VastObjectDetails objectDetails;

    @BeforeEach
    public void setup()
    {
        this.objectDetails = createObjectDetails("tableName", "handle123");
    }

    @Test
    public void testJsonRoundTrip()
    {
        VastTableHandle tableHandle = new VastTableHandle("schemaName",
                "tableName", objectDetails, false, false).forDelete();

        JsonCodec<VastTableHandle> codec = jsonCodec(VastTableHandle.class);
        String json = codec.toJson(tableHandle);
        VastTableHandle copy = codec.fromJson(json);
        assertEquals(copy, tableHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester
                .equivalenceTester()
                .addEquivalentGroup(
                        new VastTableHandle("schema", "table", objectDetails, false,
                                false),
                        new VastTableHandle("schema", "table", objectDetails, false,
                                false))
                .addEquivalentGroup(
                        new VastTableHandle("schemaX", "table", objectDetails, false,
                                false),
                        new VastTableHandle("schemaX", "table", objectDetails, false,
                                false))
                .addEquivalentGroup(
                        new VastTableHandle("schemaX", "table", objectDetails, true,
                                false),
                        new VastTableHandle("schemaX", "table", objectDetails, true,
                                false))
                .addEquivalentGroup(
                        new VastTableHandle("schema", "tableX", objectDetails, false,
                                false),
                        new VastTableHandle("schema", "tableX", objectDetails, false,
                               false)).check();
    }

    @ParameterizedTest
    @MethodSource("partitionTestProvider")
    public void testIsNonUpdateableColumnPredicate(String sourceCol, String partCol, String transform, Integer arg, String testCol, boolean expected)
    {
        // 1. Setup PartitionColumnMetadata based on parameterized inputs
        PartitionColumnMetadata partition = new PartitionColumnMetadata(
                partCol,
                "varchar",
                sourceCol,
                "varchar",
                transform,
                arg);

        // 2. Create the handle with the partition and an additional sorted column
        VastTableHandle handle = new VastTableHandle("schema",
                "table",
                createObjectDetails("table", "handle"),
                false,
                false).withPartitionColumns(List.of(
                        partition)).withSortedColumns(List.of(
                                "sorted_col"));

        // 3. Get the predicate
        Predicate<String> predicate = handle.getIsNonUpdateableColumnPredicate();

        // 4. Assert that the predicate correctly identifies the parameterized column
        assertEquals(expected, predicate.test(testCol));

        // 5. Assert that it also correctly identifies sorted columns (case-insensitive)
        Assertions.assertTrue(predicate.test("sorted_col"));
        Assertions.assertTrue(predicate.test("SORTED_COL"));

        // 6. Assert that a random column is not mistakenly identified as non-updateable
        Assertions.assertFalse(predicate.test("some_random_col"));
    }

    private static Stream<Arguments> partitionTestProvider()
    {
        return Stream.of(
                // sourceColumn, partitionColumn, transform, transformArg, testColumn, expectedResult
                Arguments.of("col1", "part1", "identity", null, "col1", true),
                Arguments.of("col2", "part2", "year", null, "COL2", true), // Tests case-insensitivity
                Arguments.of("col3", "part3", "month", null, "col3", true),
                Arguments.of("col4", "part4", "day", null, "col4", true),
                Arguments.of("col5", "part5", "hour", null, "col5", true),
                Arguments.of("col6", "part6", "bucket", 10, "col6", true),
                Arguments.of("col7", "part7", "truncate", 5, "col7", true),
                // Should return false because we are testing for a column name that doesn't match the source
                Arguments.of("col8", "part8", "identity", null, "col8_other", false));
    }
}
