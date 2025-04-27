/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.vastdata.spark.predicate.VastPredicate;
import com.vastdata.spark.predicate.VastPredicatePushdown;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.testng.annotations.Test;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.mutable.Builder;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.vastdata.spark.predicate.in.MinMaxedRangePredicate.IS_NULL_PREDICATE;
import static com.vastdata.spark.predicate.in.MinMaxedRangePredicate.MIN_MAX_PREDICATE;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestVastPredicatePushdown
{
    private LiteralValue<?> buildLit(Object value, DataType dt)
    {
        return new LiteralValue<>(value, dt);
    }

    private FieldReference buildFieldRef(String refName)
    {
        Builder<String, List<String>> objectListBuilder = List$.MODULE$.newBuilder();
        objectListBuilder.$plus$eq(refName);
        return new FieldReference(objectListBuilder.result());
    }

    private Predicate buildBinaryPred(String name, Expression a, Expression b)
    {
        return new Predicate(name, new Expression[]{a, b});
    }

    private Predicate buildUnaryPred(String name, Expression a)
    {
        return new Predicate(name, new Expression[]{a});
    }

    @Test
    public void testSanity()
    {
        StructType schema = new StructType(new StructField[] {
                createStructField("i", DataTypes.IntegerType, true)
        });

        Predicate[] predicates = {
                buildBinaryPred("=", buildFieldRef("i"), buildLit(5, DataTypes.IntegerType))
        };
        VastPredicatePushdown pushdown = VastPredicatePushdown.parse(predicates, schema);
        System.out.println("sanity-pushdown: " + pushdown.getPushedDown().get(0).get(0).getPredicate());
        assertFalse(pushdown.getPushedDown().isEmpty());
        assertTrue(pushdown.getPostFilter().isEmpty());
    }

    @Test
    public void testOrExpression()
    {
        StructType schema = new StructType(new StructField[]{
                createStructField("i", DataTypes.IntegerType, true),
        });
        Predicate[] predicates = {
                buildBinaryPred("OR",
                        buildBinaryPred("=", buildFieldRef("i"), buildLit(5, DataTypes.IntegerType)),
                        buildBinaryPred("=", buildFieldRef("i"), buildLit(3, DataTypes.IntegerType)))
        };
        VastPredicatePushdown pushdown = VastPredicatePushdown.parse(predicates, schema);
        assertEquals(pushdown.getPushedDown().get(0).size(), 2);
        assertTrue(pushdown.getPostFilter().isEmpty());
    }

    @Test
    public void testBetween()
    {
        StructType schema = new StructType(new StructField[]{
                createStructField("i", DataTypes.IntegerType, true),
        });
        Predicate[] predicates = {
                buildBinaryPred("AND",
                        buildBinaryPred(">", buildFieldRef("i"), buildLit(1, DataTypes.IntegerType)),
                        buildBinaryPred("<", buildFieldRef("i"), buildLit(10, DataTypes.IntegerType)))
        };
        VastPredicatePushdown pushdown = VastPredicatePushdown.parse(predicates, schema);
        assertEquals(pushdown.getPushedDown().get(0).size(), 1);
        assertEquals(pushdown.getPostFilter().size(), 0);
    }

    @Test
    public void testAndExpression()
    {
        StructType schema = new StructType(new StructField[]{
                createStructField("i", DataTypes.IntegerType, true),
        });
        Predicate[] predicates = {
                buildBinaryPred(">", buildFieldRef("i"), buildLit(3, DataTypes.IntegerType)),
                buildBinaryPred("<", buildFieldRef("i"), buildLit(5, DataTypes.IntegerType)),
        };
        VastPredicatePushdown pushdown = VastPredicatePushdown.parse(predicates, schema);
        assertEquals(pushdown.getPushedDown().get(0).size(), 1);
        assertEquals(pushdown.getPushedDown().get(1).size(), 1);
        assertEquals(pushdown.getPostFilter().size(), 0);
    }

    @Test
    public void testComparisons()
    {
        StructType schema = new StructType(new StructField[]{
                createStructField("i", DataTypes.IntegerType, true),
                createStructField("j", DataTypes.IntegerType, true),
                createStructField("k", DataTypes.IntegerType, true),
                createStructField("l", DataTypes.IntegerType, true)
        });
        Predicate[] predicates = {
                buildBinaryPred("<>", buildFieldRef("i"), buildLit(3, DataTypes.IntegerType)),
                buildBinaryPred("!=", buildFieldRef("j"), buildLit(5, DataTypes.IntegerType)),
                buildUnaryPred("IS_NULL", buildFieldRef("k")),
                buildUnaryPred("IS_NOT_NULL", buildFieldRef("l"))
        };
        VastPredicatePushdown pushdown = VastPredicatePushdown.parse(predicates, schema);
        assertEquals(pushdown.getPushedDown().get(0).size(), 2);
        assertEquals(pushdown.getPushedDown().get(1).size(), 2);
        assertEquals(pushdown.getPostFilter().size(), 0);
    }

    @Test
    public void testUnsupported()
    {
        StructType schema = new StructType(new StructField[]{
                createStructField("i", DataTypes.IntegerType, true),
                createStructField("j", DataTypes.IntegerType, true),
        });
        Predicate[] predicates = {
                buildBinaryPred("=", buildFieldRef("i"), buildFieldRef("j"))
        };
        VastPredicatePushdown pushdown = VastPredicatePushdown.parse(predicates, schema);
        assertEquals(pushdown.getPushedDown().size(), 0);
        assertEquals(pushdown.getPostFilter().size(), 1);
    }

    @Test
    public void testNotEqual()
    {
        StructType schema = new StructType(new StructField[]{
                createStructField("i", DataTypes.IntegerType, true),
        });
        Predicate[] predicates = {
                buildUnaryPred("IS_NOT_NULL", buildFieldRef("i")),
                buildUnaryPred("NOT", buildBinaryPred("=", buildFieldRef("i"), buildLit(5, DataTypes.IntegerType)))
        };
        VastPredicatePushdown pushdown = VastPredicatePushdown.parse(predicates, schema);
        assertEquals(pushdown.getPushedDown().size(), 2);
        assertEquals(pushdown.getPostFilter().size(), 0);
    }

    @Test
    public void testFilterCompaction()
    {
        StructType schemaInt = new StructType(new StructField[]{
                createStructField("x", DataTypes.IntegerType, true),
        });
        NamedReference refInt = FieldReference.column("x");

        // test graceful compaction
        int minInclusive = 678000;
        int maxNotInclusive = 678100;
        Predicate lteMax = new Predicate("<=", new Expression[]{refInt, new LiteralValue<>(maxNotInclusive - 1, DataTypes.IntegerType)});
        Predicate gteMin = new Predicate(">=", new Expression[]{refInt, new LiteralValue<>(minInclusive, DataTypes.IntegerType)});
        java.util.List<Predicate> expectedCompacted = Arrays.asList(MIN_MAX_PREDICATE.apply(gteMin, lteMax));
        Integer[] vals = IntStream.range(minInclusive, maxNotInclusive).boxed().toArray(Integer[]::new);
        assertFilterCompactionPositive(schemaInt, expectedCompacted, vals);

        // test graceful compaction
        expectedCompacted = Arrays.asList(MIN_MAX_PREDICATE.apply(gteMin, lteMax), IS_NULL_PREDICATE.apply(refInt));
        Integer[] valsWithNull = new Integer[vals.length + 1];
        valsWithNull[0] = null;
        System.arraycopy(vals, 0, valsWithNull, 1, vals.length);
        assertFilterCompactionPositive(schemaInt, expectedCompacted, valsWithNull);

        // test partial range compaction is skipped
        vals = IntStream.range(minInclusive, maxNotInclusive).filter(i -> i % 2 == 0).boxed().toArray(Integer[]::new);
        Predicate[] expectedFull = IntStream.range(minInclusive, maxNotInclusive).filter(i -> i % 2 == 0)
                .mapToObj(i -> new Predicate("=", new Expression[] {refInt, new LiteralValue<>(i, DataTypes.IntegerType)}))
                .toArray(Predicate[]::new);
        assertFilterCompactionNegative(schemaInt, expectedFull, vals);

        // test too short values list compaction is skipped
        minInclusive = 0;
        maxNotInclusive = 6;
        vals = IntStream.range(minInclusive, maxNotInclusive).boxed().toArray(Integer[]::new);
        expectedFull = IntStream.range(minInclusive, maxNotInclusive)
                .mapToObj(i -> new Predicate("=", new Expression[] {refInt, new LiteralValue<>(i, DataTypes.IntegerType)}))
                        .toArray(Predicate[]::new);
        assertFilterCompactionNegative(schemaInt, expectedFull, vals);

        // test unsupported type compaction is skipped
        StructType schemaString = new StructType(new StructField[]{
                createStructField("x", DataTypes.StringType, true),
        });
        NamedReference refString = FieldReference.column("x");
        ImmutableList<String> strings = ImmutableList.of("a", "b", "c", "d", "e");
        String[] stringVals = strings.toArray(new String[0]);
        expectedFull = strings
                .stream()
                .map(s -> new Predicate("=", new Expression[] {refString, new LiteralValue<>(s, DataTypes.StringType)}))
                        .toArray(Predicate[]::new);
        assertFilterCompactionNegative(schemaString, expectedFull, stringVals);
    }

    private static void assertFilterCompactionNegative(StructType schema, Predicate[] expected, Object[] vals)
    {
        Predicate[] predicates = {new In("x", vals).toV2()};
        VastPredicatePushdown pushdown = VastPredicatePushdown.parse(predicates, schema);
        assertTrue(pushdown.getPostFilter().isEmpty());
        java.util.List<VastPredicate> predicateList = Iterables.getOnlyElement(pushdown.getPushedDown());
        assertEquals(predicateList.size(), vals.length);
        Predicate[] actual = predicateList.stream().map(VastPredicate::getPredicate).toArray(Predicate[]::new);
        assertEquals(Arrays.toString(actual), Arrays.toString(expected));
    }

    private static void assertFilterCompactionPositive(StructType schema, java.util.List<Predicate> expected, Integer[] vals)
    {
        Predicate[] predicates = {new In("x", vals).toV2()};
        VastPredicatePushdown pushdown = VastPredicatePushdown.parse(predicates, schema);
        assertTrue(pushdown.getPostFilter().isEmpty());
        java.util.List<Predicate> minMaxPredicates = pushdown.getPushedDown().stream()
                .flatMap(Collection::stream)
                .map(VastPredicate::getPredicate)
                .collect(Collectors.toList());
        assertEquals(minMaxPredicates, expected);
    }
}
