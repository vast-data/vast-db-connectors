/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.execution.SparkPlan;
import scala.Tuple2;
import scala.collection.Map;
import scala.collection.Map$;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.collection.mutable.Builder;

public final class SparkPlannerUtil
{
    private SparkPlannerUtil()
    {
    }

    public static Map<String, String> getViewPropertiesMap(VastView vastView)
    {
        Builder<Tuple2<String, String>, Seq<Tuple2<String, String>>> mapBuilder = Seq$.MODULE$.newBuilder();
        java.util.Map<String, String> currentProperties = vastView.properties();
        currentProperties.entrySet().stream().filter(
                e -> !e.getKey().equals("comment")).map(
                e -> Tuple2.apply(e.getKey(), e.getValue())).forEach(
                mapBuilder::$plus$eq);
        return (Map<String, String>) Map$.MODULE$.apply(mapBuilder.result());
    }

    public static Seq<SparkPlan> getEmptySparkPlanSeq()
    {
        return (Seq<SparkPlan>) Seq$.MODULE$.<SparkPlan>empty();
    }

    public static Seq<Attribute> getEmptyAttributeSeq()
    {
        return (Seq<Attribute>) Seq$.MODULE$.<Attribute>empty();
    }

    public static Seq<InternalRow> getEmptyInternalRowSeq()
    {
        return (Seq<InternalRow>) Seq$.MODULE$.empty();
    }

    public static Seq<String> getEmptyStringSeq()
    {
        return (Seq<String>) Seq$.MODULE$.empty();
    }
}
