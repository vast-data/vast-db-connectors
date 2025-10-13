/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import org.apache.spark.sql.catalyst.expressions.AttributeMap;
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import scala.Option;
import scala.math.BigInt;

import java.lang.reflect.Field;
import java.util.function.BiConsumer;

/*
This class is being used to set plan statistics.
Currently, in Spark's 3.1 ColumnStatistics reporting are not supported, they will be added in version 3.4
In order to use the full benefits of the cost based optimize we need to inject the logical plan
 and set the self collected column statistics produced by ANALYZE COLUMN COMMAND.
https://github.com/apache/spark/commit/96843850b4f2f060c1e67e86da716db459458f7f
*/
public class SparkPlanStatisticsSetter implements BiConsumer<LogicalPlan, Statistics>
{
    @Override
    public void accept(LogicalPlan plan, Statistics statistics)
    {
        AttributeMap<ColumnStat> columnStats = statistics.attributeStats();
        long sizeInBytes = statistics.sizeInBytes().longValue();
        long rowCount = statistics.rowCount().get().longValue();
        try {
            Class<?> aClass = plan.getClass();
            while (aClass != LogicalPlan.class && aClass.getSuperclass() != null) {
                aClass = aClass.getSuperclass();
            }
            Field cacheRef = aClass.getDeclaredField("statsCache");
            cacheRef.setAccessible(true);
            Statistics x = new Statistics(BigInt.apply(sizeInBytes), Option.apply(BigInt.apply(rowCount)), columnStats, false);
            cacheRef.set(plan, Option.apply(x));
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
