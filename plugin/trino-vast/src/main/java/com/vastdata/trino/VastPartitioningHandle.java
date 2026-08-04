/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.client.partition.PartitionColumnMetadata;
import com.vastdata.trino.partition.VastPartitionFunction;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.vastdata.trino.TypeUtils.convertArrowFieldToTrinoType;

public record VastPartitioningHandle(List<VastPartitionFunction> partitionFunctions)
        implements ConnectorPartitioningHandle
{
    private static int columnNameToIndex(String columnName,
                                         List<VastColumnHandle> tableColumns)
    {
        for (int i = 0; i < tableColumns.size(); i++) {
            if (tableColumns
                    .get(i)
                    .getBaseField()
                    .getName()
                    .equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        throw new IllegalStateException(
                "Partition column not found: " + columnName);
    }

    public static VastPartitioningHandle create(List<PartitionColumnMetadata> partitionColumns,
                                                List<VastColumnHandle> tableColumns)
    {
        List<VastPartitionFunction> defs = IntStream
                .range(0, partitionColumns.size())
                .mapToObj(partitionIdx ->
                {
                    PartitionColumnMetadata pcm = partitionColumns.get(
                            partitionIdx);
                    int columnIdx = columnNameToIndex(pcm.getSourceColumnName(),
                            tableColumns);
                    Field arrowField = tableColumns
                            .get(columnIdx)
                            .getBaseField();
                    return VastPartitionFunction.create(pcm,
                            convertArrowFieldToTrinoType(arrowField),
                            partitionIdx, columnIdx);
                })
                .collect(toImmutableList());
        return new VastPartitioningHandle(defs);
    }
}
