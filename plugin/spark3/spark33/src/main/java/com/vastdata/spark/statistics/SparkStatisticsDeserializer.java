/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.spark.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import com.vastdata.client.error.VastUserException;
import ndb.NDB;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeMap$;
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat;
import org.apache.spark.sql.catalyst.plans.logical.Histogram;
import org.apache.spark.sql.catalyst.plans.logical.HistogramBin;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.mutable.Builder;
import scala.math.BigInt;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vastdata.spark.statistics.StatsUtils.fieldToAttribute;
import static java.lang.String.format;

public class SparkStatisticsDeserializer extends JsonDeserializer<Statistics> {
    private static final Logger LOG = LoggerFactory.getLogger(SparkStatisticsDeserializer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new Jdk8Module())
            .registerModule(new DefaultScalaModule())
            .registerModule(new ParameterNamesModule(JsonCreator.Mode.PROPERTIES));

    private final StructType schema;

    public SparkStatisticsDeserializer(StructType schema) {
        this.schema = schema;
    }

    @Override
    public Statistics deserialize(JsonParser jp, DeserializationContext deserializationContext)
            throws IOException
    {
        final boolean useHistogram = useHistogram();
        JsonNode node = jp.getCodec().readTree(jp);

        JsonNode sizeInBytesNode = node.get("sizeInBytes").requireNonNull();
        BigInt sizeInBytes = BigInt.apply(sizeInBytesNode.asLong());

        Option<BigInt> rowCount = getOptionBigIntFromNode(node.get("rowCount"));

        Builder<Tuple2<Attribute, ColumnStat>, List<Tuple2<Attribute, ColumnStat>>> builder = List$.MODULE$.newBuilder();

        ObjectNode columnStatistics = (ObjectNode) node.get("attributeStats");

        Consumer<Map.Entry<String, JsonNode>> entryConsumer = next -> {
            String key = next.getKey();
            JsonNode statsNode = next.getValue();
            String fieldName = key.substring(0, key.indexOf('#'));
            StructField structField = this.schema.apply(fieldName);
            try {
                // Spark adds '#' in column name attribute, we need to strip it in order to match colum
                Attribute attribute = fieldToAttribute(structField, schema);
                Option<BigInt> distinctCount = getOptionBigIntFromNode(statsNode.get("distinctCount"));
                Option<BigInt> nullCount = getOptionBigIntFromNode(statsNode.get("nullCount"));
                Option<Object> avgLen = getLongValueByKeyName(statsNode, "avgLen");
                Option<Object> maxLen = getLongValueByKeyName(statsNode, "maxLen");
                DataType dataType = structField.dataType();
                Option<Object> min = getDataTypeValueByKeyName(statsNode, "min", dataType);
                Option<Object> max = getDataTypeValueByKeyName(statsNode, "max", dataType);
                ColumnStat colStats;
                if (useHistogram && statsNode.has("histogram") && ! (statsNode.get("histogram") instanceof NullNode)) {
                    JsonNode histogramNode = statsNode.get("histogram");
                    Option<Object> height = getDataTypeValueByKeyName(histogramNode, "height", DataTypes.DoubleType);
                    ArrayNode binsNode = (ArrayNode) histogramNode.get("bins");
                    java.util.List<HistogramBin> bins = new ArrayList<>();
                    binsNode.forEach(bin -> {
                        Option<Object> lo = getDataTypeValueByKeyName(bin, "lo", DataTypes.DoubleType);
                        Option<Object> hi = getDataTypeValueByKeyName(bin, "hi", DataTypes.DoubleType);
                        Option<Object> ndv = getDataTypeValueByKeyName(bin, "ndv", DataTypes.LongType);
                        HistogramBin histogramBin = new HistogramBin((Double) lo.get(), (Double) hi.get(), (Long) ndv.get());
                        bins.add(histogramBin);
                    });
                    Histogram histogram = new Histogram((Double) height.get(), bins.toArray(new HistogramBin[0]));
                    colStats = new ColumnStat(distinctCount, min, max, nullCount, avgLen, maxLen, Option.apply(histogram), 0);
                }
                else {
                    colStats = new ColumnStat(distinctCount, min, max, nullCount, avgLen, maxLen, Option.empty(), 0);
                }

                builder.$plus$eq(Tuple2.apply(attribute, colStats));
            }
            catch (Throwable any) {
                throw new RuntimeException(format("Failed parsing column %s stats node of type %s: %s", key, structField, statsNode), any);
            }
        };
        columnStatistics.fields().forEachRemaining(entryConsumer);
        return new Statistics(sizeInBytes, rowCount, AttributeMap$.MODULE$.apply(builder.result()), false);
    }

    private static boolean useHistogram()
    {
        try {
            return NDB.getConfig().getUseColumnHistogram();
        }
        catch (VastUserException t) {
            LOG.warn("Failed getting useHistogram conf. Defaulting to true", t);
            return true;
        }
    }

    private Option<Object> getDataTypeValueByKeyName(JsonNode statsNode, String keyName, DataType dataType)
    {
        JsonNode node = statsNode.get(keyName);
        if (node == null) {
            return Option.empty();
        }
        else {
            try {
                return getValueFromNode(node, dataType);
            }
            catch (Exception any) {
                LOG.error("Failed parsing key: {}, of node: {}, of type: {}, defaulting to empty", keyName, dataType, statsNode);
                return Option.empty();
            }
        }
    }

    private Option<Object> getLongValueByKeyName(JsonNode statsNode, String keyName)
    {
        JsonNode node = statsNode.get(keyName);
        if (node == null) {
            return Option.empty();
        }
        else {
            try {
                return Option.apply(node.asLong());
            }
            catch (Exception any) {
                LOG.error("Failed parsing key: {}, of node: {}, defaulting to empty", keyName, statsNode);
                return Option.empty();
            }
        }
    }

    private Option<BigInt> getOptionBigIntFromNode(JsonNode node)
    {
        if (node == null) {
            return Option.empty();
        } else {
//            TODO: verify the value is long compatible
            return Option.apply(BigInt.apply(node.asLong()));
        }
    }

    private Option<Object> getValueFromNode(JsonNode valueNode, DataType dataType)
    {
        String content = valueNode.toString();
        Object x = extractValue(dataType, content);
        return Option.apply(x);
    }

    private Object extractValue(DataType dataType, String content)
    {
        return getTypeHandler(dataType).apply(content);
    }

    private Function<String, Object> getTypeHandler(DataType dataType)
    {
        for (Map.Entry<Predicate<DataType>, Function<String, Object>> entry: typesMap.entrySet()) {
            if (entry.getKey().test(dataType)) {
                return entry.getValue();
            }
        }
        throw new UnsupportedOperationException(format("Unsupported type: %s", dataType));
    }

    // used as a container of (key, value) pairs (not used for lookup).
    private static final Map<Predicate<DataType>, Function<String, Object>> typesMap = new HashMap<>();
    static {
        typesMap.put(dataType -> dataType instanceof IntegerType, new TypedValueReader(Integer.class));
        typesMap.put(dataType -> dataType instanceof LongType, new TypedValueReader(Long.class));
        typesMap.put(dataType -> dataType instanceof ShortType, new TypedValueReader(Short.class));
        typesMap.put(dataType -> dataType instanceof DoubleType, new TypedValueReader(Double.class));
        typesMap.put(dataType -> dataType instanceof FloatType, new TypedValueReader(Float.class));
        typesMap.put(dataType -> dataType instanceof ByteType, new TypedValueReader(Byte.class));
        typesMap.put(dataType -> dataType instanceof BooleanType, new TypedValueReader(Boolean.class));
        typesMap.put(dataType -> dataType instanceof DecimalType, new DecimalValueReader());
        typesMap.put(dataType -> dataType instanceof DateType, new TypedValueReader(Integer.class));
        typesMap.put(dataType -> dataType instanceof TimestampType, new TypedValueReader(Long.class));
        typesMap.put(dataType -> dataType instanceof StringType, new TypedValueReader(UTF8String.class));
        typesMap.put(dataType -> dataType instanceof CharType, new TypedValueReader(UTF8String.class));
        typesMap.put(dataType -> dataType instanceof BinaryType, new TypedValueReader(byte[].class));
    }

    private static class TypedValueReader
            implements Function<String, Object> {
        private static final Logger TYPE_READER_LOG = LoggerFactory.getLogger(TypedValueReader.class);
        private final Class<?> valueType;

        private TypedValueReader(Class<?> valueType) {
            this.valueType = valueType;
        }

        @Override
        public Object apply(String content)
        {
            try {
                Object o = OBJECT_MAPPER.readValue(content, valueType);
                if (TYPE_READER_LOG.isDebugEnabled()) {
                    TYPE_READER_LOG.debug("TYPE_READER_LOG for class {} read value: {}", this.valueType, o);
                }
                return o;
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(format("Failed parsing value node: %s, with expected type: %s", content, valueType), e);
            }
        }
    }

    private static class DecimalValueReader extends TypedValueReader
    {

        private DecimalValueReader()
        {
            super(BigDecimal.class);
        }

        @Override
        public Object apply(String content)
        {
            Object res = super.apply(content);
            if (res == null) {
                return null;
            }
            else {
                return Decimal.apply((BigDecimal) res);
            }
        }
    }
}
