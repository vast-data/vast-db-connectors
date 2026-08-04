/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.partition;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Arrays;
import java.util.List;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;

public final class PartitionColumnMetadata
{
    public final String columnName;
    public final String columnType;
    public final String sourceColumnName;
    public final String sourceColumnType;
    public final String transform;
    public final Integer arg;

    public PartitionColumnMetadata(
            @JsonProperty("column_name") String columnName,
            @JsonProperty("column_type") String columnType,
            @JsonProperty("source_column_name") String sourceColumnName,
            @JsonProperty("source_column_type") String sourceColumnType,
            @JsonProperty("transform") String transform,
            @JsonProperty("arg") Integer arg)
    {
        this.columnName = columnName;
        this.columnType = columnType;
        this.sourceColumnName = sourceColumnName;
        this.sourceColumnType = sourceColumnType;
        this.transform = transform;
        this.arg = arg;
    }

    public static List<PartitionColumnMetadata> parse(Schema schema)
    {
        String partitionColumnsJson = schema
                .getCustomMetadata()
                .get(PartitionConstants.PARTITION_SPEC);

        if (partitionColumnsJson == null) {
            return List.of();
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            PartitionColumnMetadata[] partitionColumns = mapper.readValue(
                    partitionColumnsJson, PartitionColumnMetadata[].class);

            if (partitionColumns.length == 0) {
                throw new IllegalArgumentException(
                        "Partition specification with 0 partition columns");
            }

            return Arrays.asList(partitionColumns);
        }
        catch (JsonProcessingException e) {
            throw toRuntime("Failed to parse partition spec from server", e);
        }
    }

    @JsonProperty("column_name")
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty("column_type")
    public String getColumnType()
    {
        return columnType;
    }

    @JsonProperty("source_column_name")
    public String getSourceColumnName()
    {
        return sourceColumnName;
    }

    @JsonProperty("source_column_type")
    public String getSourceColumnType()
    {
        return sourceColumnType;
    }

    @JsonProperty("transform")
    public String getTransform()
    {
        return transform;
    }

    @JsonProperty("arg")
    public String getArg()
    {
        return arg == null ? null : arg.toString();
    }

    @Override
    public String toString()
    {
        return String.format(
                "PartitionColumnMetadata{columnName: %s, columnType: %s, sourceColumnName: %s, sourceColumnType: %s, transform: %s, arg: %d}",
                columnName, columnType, sourceColumnName, sourceColumnType,
                transform, arg);
    }
}
