/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;

import java.util.function.LongSupplier;

public class SparkQueryIDSupplier
        implements LongSupplier
{
    @Override
    public long getAsLong()
    {
        SparkContext sparkContext = SparkContext$.MODULE$.getActive().get();
        String execId = sparkContext.getLocalProperty("spark.sql.execution.id");
        if (execId == null) {
            throw new RuntimeException("Did not find a value for spark.sql.execution.id");
        }
        return Long.parseLong(execId);
    }
}
