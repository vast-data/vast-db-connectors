/* Copyright (C) Vast Data Ltd. */

package com.vastdata.trino.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import io.trino.spi.statistics.Estimate;

import java.io.IOException;

public final class TrinoStatisticsMapper
{
    private TrinoStatisticsMapper() {}

    static ObjectMapper instance()
    {
        return new ObjectMapper()
                .registerModule(new Jdk8Module())
                .registerModule(new ParameterNamesModule(JsonCreator.Mode.PROPERTIES))
                .registerModule(new SimpleModule()
                        .addDeserializer(TrinoSerializableTableStatistics.class, new TrinoSerializableTableStatisticsDeserializer())
                        .addSerializer(Estimate.class, new EstimateSerializer())
                        );
    }

    private static class EstimateSerializer extends JsonSerializer<Estimate>
    {
        @Override
        public void serialize(Estimate value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException
        {
            gen.writeNumber(value.getValue());
        }
    }
}
