/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.statistics;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.vastdata.trino.VastColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class TrinoSerializableTableStatisticsDeserializer extends JsonDeserializer
{
    public TrinoSerializableTableStatisticsDeserializer() {}

    private static final ObjectMapper mapper = TrinoStatisticsMapper.instance();

    public TrinoSerializableTableStatistics deserialize(JsonParser jp, DeserializationContext deserializationContext)
            throws IOException
    {
        List<TrinoSerializableTableStatistics.Entry<ColumnHandle, ColumnStatistics>> pairs = new LinkedList<>();
        JsonNode node = jp.getCodec().readTree(jp);

        JsonNode rowCount = node.get("rowCount");
        Estimate est = getEstimationNode(rowCount);

        ArrayNode pairList = (ArrayNode) node.get("pairList");
        Iterator<JsonNode> entryIterator = pairList.iterator();
        while (entryIterator.hasNext()) {
            JsonNode next = entryIterator.next();
            JsonParser key = next.get("key").traverse();

            VastColumnHandle vastColumnHandle = mapper.readValue(key, VastColumnHandle.class);
            JsonNode statsNode = next.get("value");
            Estimate nullsFraction = getEstimationNode(statsNode.get("nullsFraction"));
            Estimate distinctValueCount = getEstimationNode(statsNode.get("distinctValuesCount"));
            Estimate dataSize = getEstimationNode(statsNode.get("dataSize"));

            JsonNode rangeNode = statsNode.get("range");
            String rangeNodeString = rangeNode.toString();
            Optional<DoubleRange> r;
            if (rangeNode.isNull()) {
                r = Optional.empty();
            }
            else {
                r = Optional.of(mapper.readValue(rangeNodeString, DoubleRange.class));
            }
            ColumnStatistics stats = new ColumnStatistics(nullsFraction, distinctValueCount, dataSize, r);
            pairs.add(new TrinoSerializableTableStatistics.Entry<ColumnHandle, ColumnStatistics>(vastColumnHandle, stats));
        }
        return new TrinoSerializableTableStatistics(est, pairs);
    }

    private Estimate getEstimationNode(JsonNode node)
    {
        if (node.isDouble()) {
            return Estimate.of(node.doubleValue());
        }
        else {
            return Estimate.unknown();
        }
    }
}
