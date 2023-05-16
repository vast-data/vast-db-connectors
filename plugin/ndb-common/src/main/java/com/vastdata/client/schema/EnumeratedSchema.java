/* Copyright (C) Vast Data Ltd. */

package com.vastdata.client.schema;

import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class EnumeratedSchema
{
    private static class EnumeratedField
    {
        private final int index; // enumerated using pre-order depth-first traversal over the whole schema
        private final List<EnumeratedField> children;

        public EnumeratedField(int index, List<EnumeratedField> children)
        {
            this.index = index;
            this.children = children;
        }

        public void collectLeafIndices(Consumer<Integer> resultsConsumer)
        {
            if (children.isEmpty()) {
                resultsConsumer.accept(index);
            }
            else {
                children.forEach(child -> child.collectLeafIndices(resultsConsumer));
            }
        }

        @Override
        public String toString()
        {
            return "EnumeratedField{" +
                    "index=" + index +
                    ", children=" + children +
                    '}';
        }
    }

    private final Schema schema;
    private final List<EnumeratedField> nodes;
    private final Map<String, Integer> baseFieldByName;
    private int nodeCount;

    public EnumeratedSchema(Collection<Field> baseFields)
    {
        this.nodeCount = 0;
        this.nodes = baseFields.stream().map(this::visitField).collect(Collectors.toList());
        this.schema = new Schema(baseFields);

        ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
        int i = 0;
        for (Field baseField : baseFields) {
            builder.put(baseField.getName(), i++);
        }
        this.baseFieldByName = builder.build();
    }

    // Enumerate fields using pre-order depth-first traversal
    private EnumeratedField visitField(Field field)
    {
        int current = nodeCount++;
        List<EnumeratedField> children = field.getChildren().stream().map(this::visitField).collect(Collectors.toList());
        return new EnumeratedField(current, children);
    }

    public void collectProjectionIndices(String baseFieldName, List<Integer> projectionPath, Consumer<Integer> resultsConsumer)
    {
        int baseFieldIndex = baseFieldByName.get(baseFieldName);
        EnumeratedField field = nodes.get(baseFieldIndex);
        for (int projection : projectionPath) {
            field = field.children.get(projection);
        }
        field.collectLeafIndices(resultsConsumer);
    }

    public int getBaseFieldIndexByName(String baseFieldName)
    {
        return baseFieldByName.get(baseFieldName);
    }

    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public String toString()
    {
        return schema.toString();
    }
}
