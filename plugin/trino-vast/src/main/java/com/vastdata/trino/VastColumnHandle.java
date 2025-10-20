/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableList;
import com.vastdata.trino.expression.VastExpression;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.vastdata.trino.TypeUtils.convertArrowFieldToTrinoType;
import static java.util.Objects.requireNonNull;

public final class VastColumnHandle
        implements ColumnHandle
{
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectWriter writer = mapper.writerFor(Field.class);
    private static final ObjectReader reader = mapper.readerFor(Field.class);

    private String serializedField;

    private Field deserializedBaseField;
    private Field deserializedProjectedField;

    // The list of field indices to the projected part of the top-level column, see ConnectorMetadata#applyProjection for more details.
    // For now, we only support row dereference pushdown,  i.e. `SELECT a.x WHERE b.y = 0` should only read `a.x` and `b.y`.
    // It works by replacing the dereferenced subcolumns by synthetic column handles, to be used by the planner (for predicate pushdown) and our page source.
    private final List<Integer> projectionPath;

    // Computed over this column if set (TODO: allow multi-column expressions, e.g. 'x > y')
    private final VastExpression expression;

    public VastColumnHandle(Field field)
    {
        this.deserializedBaseField = field;
        this.projectionPath = List.of();
        this.expression = null;
    }

    public static VastColumnHandle fromField(Field field)
    {
        try {
            return new VastColumnHandle(writer.writeValueAsString(field), List.of(), null);
        }
        catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize " + field, e);
        }
    }

    @JsonCreator
    public VastColumnHandle(
            @JsonProperty("serializedField") String serializedField,
            @JsonProperty("projectionPath") List<Integer> projectionPath,
            @JsonProperty("expression") VastExpression expression)
    {
        this.serializedField = serializedField;
        this.projectionPath = ImmutableList.copyOf(projectionPath);
        this.expression = expression;
    }

    public VastColumnHandle withProjectionPath(List<Integer> projectionPath)
    {
        return new VastColumnHandle(
                this.serializedField,
                ImmutableList.<Integer>builder().addAll(this.projectionPath).addAll(projectionPath).build(),
                this.expression);
    }

    public VastColumnHandle withProjectionExpression(VastExpression expression)
    {
        requireNonNull(expression.getFunction());
        return new VastColumnHandle(
                this.serializedField,
                this.projectionPath,
                expression);
    }

    @JsonProperty
    public String getSerializedField()
    {
        if (serializedField == null) {
            try {
                serializedField = writer.writeValueAsString(deserializedBaseField);
            }
            catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Failed to serialize " + deserializedBaseField, e);
            }
        }
        return serializedField;
    }

    @JsonProperty
    public List<Integer> getProjectionPath()
    {
        return projectionPath;
    }

    @JsonProperty
    public VastExpression getExpression()
    {
        return expression;
    }

    @JsonIgnore
    public Field getBaseField()
    {
        if (deserializedBaseField == null) {
            try {
                deserializedBaseField = reader.readValue(serializedField);
            }
            catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Failed to deserialize " + serializedField, e);
            }
        }
        return deserializedBaseField;
    }

    @JsonIgnore
    public Field getField()
    {
        if (deserializedProjectedField == null) {
            Field field = getBaseField();
            for (int childIndex : projectionPath) {
                field = field.getChildren().get(childIndex);
            }
            deserializedProjectedField = field;
        }
        return deserializedProjectedField;
    }

    @JsonIgnore
    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(getField().getName())
                .setType(getType())
                .setHidden(false)
                .build();
    }

    @JsonIgnore
    public ColumnSchema getColumnSchema()
    {
        return ColumnSchema.builder(getColumnMetadata()).build();
    }

    private Type getType()
    {
        return convertArrowFieldToTrinoType(getField());
    }

    public VastColumnHandle stripMetadata()
    {
        Field field = this.getField();
        FieldType originalFieldType = field.getFieldType();
        FieldType fieldType = new FieldType(originalFieldType.isNullable(), originalFieldType.getType(), originalFieldType.getDictionary());
        return new VastColumnHandle(new Field(field.getName(), fieldType, field.getChildren()));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getBaseField(), getProjectionPath(), getExpression());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        VastColumnHandle other = (VastColumnHandle) obj;
        return getBaseField().equals(other.getBaseField())
                && getProjectionPath().equals(other.getProjectionPath())
                && Objects.equals(getExpression(), other.getExpression());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("field", serializedField)
                .add("path", projectionPath)
                .add("expr", expression)
                .toString();
    }
}
