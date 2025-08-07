/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.vastdata.spark.predicate.VastPredicate;
import com.vastdata.spark.predicate.VastPredicatePushdown;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.testng.annotations.Test;
import scala.collection.immutable.List$;
import scala.collection.mutable.Builder;

import java.util.List;

import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;

public class TestVastPredicate
{
    private LiteralValue<?> buildLit(Object value, DataType dt)
    {
        return new LiteralValue<>(value, dt);
    }

    private FieldReference buildFieldRef(String refName)
    {
        Builder<String, scala.collection.immutable.List<String>> objectListBuilder = List$.MODULE$.newBuilder();
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

        Predicate[] predicates1 = {
                buildBinaryPred("=", buildFieldRef("i"), buildLit(5, DataTypes.IntegerType))
        };
        List<List<VastPredicate>> pushdown1 = VastPredicatePushdown.parse(predicates1, schema).getPushedDown();
        List<List<VastPredicate>> pushdown2 = VastPredicatePushdown.parse(predicates1, schema).getPushedDown();
        Predicate[] predicates3 = {
                buildBinaryPred("=", buildFieldRef("i"), buildLit(6, DataTypes.IntegerType))
        };
        List<List<VastPredicate>> pushdown3 = VastPredicatePushdown.parse(predicates3, schema).getPushedDown();
        assertEquals(pushdown2.hashCode(), pushdown1.hashCode());
        assertEquals(pushdown2, pushdown1);
        assertNotSame(pushdown1, pushdown2);
        assertNotEquals(pushdown3, pushdown1);
    }

    @Test
    public void testOrExpression()
    {
        StructType schema = new StructType(new StructField[]{
                createStructField("i", DataTypes.IntegerType, true),
        });
        Predicate[] predicates1 = {
                buildBinaryPred("OR",
                        buildBinaryPred("=", buildFieldRef("i"), buildLit(5, DataTypes.IntegerType)),
                        buildBinaryPred("=", buildFieldRef("i"), buildLit(3, DataTypes.IntegerType)))
        };
        List<List<VastPredicate>> pushdown1 = VastPredicatePushdown.parse(predicates1, schema).getPushedDown();
        List<List<VastPredicate>> pushdown2 = VastPredicatePushdown.parse(predicates1, schema).getPushedDown();
        Predicate[] predicates3 = {
                buildBinaryPred("OR",
                        buildBinaryPred("=", buildFieldRef("i"), buildLit(5, DataTypes.IntegerType)),
                        buildBinaryPred("=", buildFieldRef("i"), buildLit(4, DataTypes.IntegerType)))
        };
        List<List<VastPredicate>> pushdown3 = VastPredicatePushdown.parse(predicates3, schema).getPushedDown();
        assertEquals(pushdown2.hashCode(), pushdown1.hashCode());
        assertEquals(pushdown1, pushdown2);
        assertNotSame(pushdown1, pushdown2);
        assertNotEquals(pushdown3, pushdown1);
    }

    @Test
    public void testBetween()
    {
        StructType schema = new StructType(new StructField[]{
                createStructField("i", DataTypes.IntegerType, true),
        });
        Predicate[] predicates1 = {
                buildBinaryPred("AND",
                        buildBinaryPred(">", buildFieldRef("i"), buildLit(1, DataTypes.IntegerType)),
                        buildBinaryPred("<", buildFieldRef("i"), buildLit(10, DataTypes.IntegerType)))
        };
        List<List<VastPredicate>> pushdown1 = VastPredicatePushdown.parse(predicates1, schema).getPushedDown();
        List<List<VastPredicate>> pushdown2 = VastPredicatePushdown.parse(predicates1, schema).getPushedDown();
        Predicate[] predicates3 = {
                buildBinaryPred("AND",
                        buildBinaryPred(">", buildFieldRef("i"), buildLit(2, DataTypes.IntegerType)),
                        buildBinaryPred("<", buildFieldRef("i"), buildLit(10, DataTypes.IntegerType)))
        };
        List<List<VastPredicate>> pushdown3 = VastPredicatePushdown.parse(predicates3, schema).getPushedDown();
        assertEquals(pushdown2.hashCode(), pushdown1.hashCode());
        assertEquals(pushdown1, pushdown2);
        assertNotSame(pushdown1, pushdown2);
        assertNotEquals(pushdown3, pushdown1);
    }

    @Test
    public void testAndExpression()
    {
        StructType schema = new StructType(new StructField[]{
                createStructField("i", DataTypes.IntegerType, true),
        });
        Predicate[] predicates1 = {
                buildBinaryPred(">", buildFieldRef("i"), buildLit(3, DataTypes.IntegerType)),
                buildBinaryPred("<", buildFieldRef("i"), buildLit(5, DataTypes.IntegerType)),
        };
        List<List<VastPredicate>> pushdown1 = VastPredicatePushdown.parse(predicates1, schema).getPushedDown();
        List<List<VastPredicate>> pushdown2 = VastPredicatePushdown.parse(predicates1, schema).getPushedDown();
        Predicate[] predicates3 = {
                buildBinaryPred(">", buildFieldRef("i"), buildLit(3, DataTypes.IntegerType)),
                buildBinaryPred("<", buildFieldRef("i"), buildLit(6, DataTypes.IntegerType)),
        };
        List<List<VastPredicate>> pushdown3 = VastPredicatePushdown.parse(predicates3, schema).getPushedDown();
        assertEquals(pushdown2.hashCode(), pushdown1.hashCode());
        assertEquals(pushdown1, pushdown2);
        assertNotSame(pushdown1, pushdown2);
        assertNotEquals(pushdown3, pushdown1);
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
        Predicate[] predicates1 = {
                buildBinaryPred("<>", buildFieldRef("i"), buildLit(3, DataTypes.IntegerType)),
                buildBinaryPred("!=", buildFieldRef("j"), buildLit(5, DataTypes.IntegerType)),
                buildUnaryPred("IS_NULL", buildFieldRef("k")),
                buildUnaryPred("IS_NOT_NULL", buildFieldRef("l"))
        };
        List<List<VastPredicate>> pushdown1 = VastPredicatePushdown.parse(predicates1, schema).getPushedDown();
        List<List<VastPredicate>> pushdown2 = VastPredicatePushdown.parse(predicates1, schema).getPushedDown();
        Predicate[] predicates3 = {
                buildBinaryPred("<>", buildFieldRef("i"), buildLit(3, DataTypes.IntegerType)),
                buildBinaryPred("!=", buildFieldRef("j"), buildLit(5, DataTypes.IntegerType)),
                buildUnaryPred("IS_NOT_NULL", buildFieldRef("k")),
                buildUnaryPred("IS_NOT_NULL", buildFieldRef("l"))
        };
        List<List<VastPredicate>> pushdown3 = VastPredicatePushdown.parse(predicates3, schema).getPushedDown();
        assertEquals(pushdown2.hashCode(), pushdown1.hashCode());
        assertEquals(pushdown1, pushdown2);
        assertNotSame(pushdown1, pushdown2);
        assertNotEquals(pushdown3, pushdown1);
    }

    @Test
    public void testNotEqual()
    {
        StructType schema = new StructType(new StructField[]{
                createStructField("i", DataTypes.IntegerType, true),
        });
        Predicate[] predicates1 = {
                buildUnaryPred("IS_NOT_NULL", buildFieldRef("i")),
                buildUnaryPred("NOT", buildBinaryPred("=", buildFieldRef("i"), buildLit(5, DataTypes.IntegerType)))
        };
        List<List<VastPredicate>> pushdown1 = VastPredicatePushdown.parse(predicates1, schema).getPushedDown();
        Predicate[] predicates2 = {
                buildUnaryPred("IS_NOT_NULL", buildFieldRef("i")),
                buildBinaryPred("<>", buildFieldRef("i"), buildLit(5, DataTypes.IntegerType))
        };
        List<List<VastPredicate>> pushdown2 = VastPredicatePushdown.parse(predicates1, schema).getPushedDown();
        assertEquals(pushdown2.hashCode(), pushdown1.hashCode());
        assertEquals(pushdown1, pushdown2);
        assertNotSame(pushdown1, pushdown2);
    }
}
