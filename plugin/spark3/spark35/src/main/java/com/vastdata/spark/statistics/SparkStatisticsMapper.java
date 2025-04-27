/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.types.Decimal;

public final class SparkStatisticsMapper
{
    private SparkStatisticsMapper() {}

    static ObjectMapper instance(Table table)
    {
        return new ObjectMapper()
                .registerModule(new Jdk8Module())
                .registerModule(new DefaultScalaModule())
                .registerModule(new ParameterNamesModule(JsonCreator.Mode.PROPERTIES))
                .registerModule(new SimpleModule()
                        .addDeserializer(Statistics.class, new SparkStatisticsDeserializer(table.schema()))
                        .addSerializer(Decimal.class, SparkDecimalSerializer.getInstance()));
    }
}
