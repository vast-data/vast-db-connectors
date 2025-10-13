/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.vastdata.client.error.VastUserException;
import com.vastdata.spark.predicate.VastPredicate;
import ndb.NDB;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.AlwaysFalse;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.mockito.Mock;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.vastdata.spark.AlwaysFalseFilterUtil.isAlwaysFalsePredicate;
import static com.vastdata.spark.SparkTestUtils.getTestConfig;
import static com.vastdata.spark.predicate.in.MinMaxedRangePredicate.IS_NULL_PREDICATE;
import static com.vastdata.spark.predicate.in.MinMaxedRangePredicate.MIN_MAX_PREDICATE;
import static java.lang.String.format;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestVastScan
{
    public static final String COL_INT = "col_int";
    public static final String COL_INT2 = "col_int2";
    public static final String COL_FLOAT = "col_float";
    public static final String COL_STR = "col_str";
    public static final String COL_BIGINT = "col_bigint";
    @Mock VastTable tableMock;
    private VastScan unit;

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

    @BeforeClass
    public void initNDB()
    {
        NDB.init(getTestConfig());
    }

    @BeforeMethod
    public void clearMockServer()
            throws VastUserException
    {
        when(tableMock.name()).thenReturn("dummy");
        StructType schema = new StructType(new StructField[] {
                createStructField(COL_INT, DataTypes.IntegerType, true),
                createStructField(COL_INT2, DataTypes.IntegerType, true),
                createStructField(COL_FLOAT, DataTypes.FloatType, true),
                createStructField(COL_STR, DataTypes.StringType, true),
                createStructField(COL_BIGINT, DataTypes.LongType, true),
        });
        unit = new VastScan(777, tableMock, schema, 2000, new ArrayList<>(0));
    }

    @Test
    public void testFilterReset()
    {
        In nonEmptyIn1 = new In(COL_INT, new Integer[] {5, 6});
        Object[] vals = new Object[1000];
        Arrays.fill(vals, 8);
        In nonEmptyIn2 = new In(COL_INT2, vals);
        Filter[] filters = new Filter[] {nonEmptyIn1, nonEmptyIn2};
        unit.filter(filters);
        assertTrue(unit.pushDownPredicates.size() > 1, format("Expected multiple entries in pushed predicates list: %s", unit.pushDownPredicates));
        assertFalse(isAlwaysFalsePredicate(unit.pushDownPredicates));

        testEmptyInFilter(); // make sure existing filters are rest by empty filter
    }

    @Test
    public void testEmptyInFilter()
    {
        Filter[] emptyIn = {new In(COL_INT, new Object[] {})};
        unit.filter(emptyIn);
        List<VastPredicate> vastPredicates = Iterables.getOnlyElement(unit.pushDownPredicates);
        VastPredicate predicate = Iterables.getOnlyElement(vastPredicates);
        assertTrue(predicate.getPredicate() instanceof AlwaysFalse);
        assertTrue(isAlwaysFalsePredicate(unit.pushDownPredicates));
    }

    @Test
    public void testGracefulFilterCompaction()
    {
        Integer[] intVals = IntStream.range(0, 50).boxed().toArray(Integer[]::new);
        assertMinMaxCompaction(intVals, COL_INT, DataTypes.IntegerType, 49, 0, false);
    }

    @Test
    public void testFilterCompactionSkipPartialRange()
    {
        Integer[] intVals = IntStream.range(0, 50).boxed().filter(i -> i % 2 == 0).toArray(Integer[]::new);
        assertSkippedFilterCompactionResult(intVals, intVals.length, COL_INT);
    }

    @Test
    public void testFilterCompactionPartialListMinMaxOpt()
    {
        Integer[] intVals = IntStream.range(0, 500).boxed().filter(i -> i % 2 == 0).toArray(Integer[]::new);
        intVals[0] = null; // null, 2, 4, ..., 498
        assertMinMaxCompaction(intVals, COL_INT, DataTypes.IntegerType, intVals[intVals.length - 1], 2, true);
    }

    @Test
    public void testFilterCompactionTooLongList()
    {
        Integer[] intVals = IntStream.range(0, 3000).boxed().toArray(Integer[]::new);
        assertSkippedFilterCompactionResult(intVals, 0, COL_INT);
    }

    @Test
    public void testFilterCompactionSkipTooShortList()
    {
        Integer[] intVals = IntStream.range(0, 4).boxed().toArray(Integer[]::new);
        assertSkippedFilterCompactionResult(intVals, 4, COL_INT);
    }

    @Test
    public void testFilterCompactionSkipFloatShortList()
    {
        Float[] vals = IntStream.range(0, 4).mapToObj(i -> (float) i).toArray(Float[]::new);
        assertSkippedFilterCompactionResult(vals, 4, COL_FLOAT);
    }

    @Test
    public void testFilterCompactionSkipFloatFullList()
    {
        Float[] vals = IntStream.range(0, 50).mapToObj(i -> (float) i).toArray(Float[]::new);
        assertSkippedFilterCompactionResult(vals, 50, COL_FLOAT);
    }

    @Test
    public void testFilterCompactionSkipFloatPartialListMinMaxOpt()
    {
        Float[] vals = IntStream.range(0, 150).mapToObj(i -> (float) i).toArray(Float[]::new);
        assertSkippedFilterCompactionResult(vals, 150, COL_FLOAT);
    }

    @Test
    public void testFilterCompactionSkipLongIntegerOverflow()
    {
        Long[] vals = new Long[] {Long.MAX_VALUE, 0L, -Long.MAX_VALUE};
        assertSkippedFilterCompactionResult(vals, 3, COL_BIGINT);
    }

    private <T> void assertSkippedFilterCompactionResult(T[] valsArr, int expected, String colName)
    {
        unit.filter(new Filter[] {new In(colName, valsArr)});
        List<Predicate> minMaxPredicates = unit.pushDownPredicates.stream()
                .flatMap(Collection::stream)
                .map(VastPredicate::getPredicate)
                .collect(Collectors.toList());
        assertEquals(minMaxPredicates.size(), expected);
        List<T> actualFilterValuesList = minMaxPredicates.stream().map(p -> ((LiteralValue<T>) p.children()[1]).value()).collect(Collectors.toList());
        List<T> valsList = Lists.newArrayList(valsArr);
        if (expected > 0) {
            assertEquals(actualFilterValuesList, valsList);
        }
    }

    private <T> void assertMinMaxCompaction(T[] vals, String colName, DataType type, T max, T min, boolean hasNull)
    {
        unit.filter(new Filter[] {new In(colName, vals)});
        List<Predicate> minMaxPredicates = unit.pushDownPredicates.stream()
                .flatMap(Collection::stream)
                .map(VastPredicate::getPredicate)
                .collect(Collectors.toList());
        NamedReference ref = FieldReference.column(colName);
        Predicate lteMax = new Predicate("<=", new Expression[]{ref, new LiteralValue<>(max, type)});
        Predicate gteMin = new Predicate(">=", new Expression[]{ref, new LiteralValue<>(min, type)});
        List<Predicate> expectedCompacted = hasNull ?
                Arrays.asList(MIN_MAX_PREDICATE.apply(gteMin, lteMax), IS_NULL_PREDICATE.apply(ref)) :
                Arrays.asList(MIN_MAX_PREDICATE.apply(gteMin, lteMax));
        assertEquals(minMaxPredicates, expectedCompacted, format("Actual: %s, Expected: %s", minMaxPredicates, expectedCompacted));
    }
}
