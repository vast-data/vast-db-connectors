/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.spark.sql.types.Decimal;

import java.io.IOException;
import java.math.BigDecimal;

public final class SparkDecimalSerializer extends JsonSerializer<Decimal>
{
    private static final SparkDecimalSerializer instance = new SparkDecimalSerializer();
    private SparkDecimalSerializer() {}

    public static SparkDecimalSerializer getInstance()
    {
        return instance;
    }


    @Override
    public void serialize(Decimal decimal, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException
    {
        BigDecimal bigDecimal = decimal.toJavaBigDecimal();
        JsonSerializer<Object> valueSerializer = serializerProvider.findValueSerializer(BigDecimal.class);
        valueSerializer.serialize(bigDecimal, jsonGenerator, serializerProvider);
    }
}
