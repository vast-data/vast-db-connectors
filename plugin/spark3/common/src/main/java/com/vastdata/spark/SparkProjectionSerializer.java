/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.collect.ImmutableList;
import com.vastdata.client.VastExpressionSerializer;
import com.vastdata.client.schema.EnumeratedSchema;
import org.apache.arrow.computeir.flatbuf.Project;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collection;
import java.util.LinkedHashSet;

public class SparkProjectionSerializer
        extends VastExpressionSerializer
{
    private final Collection<Integer> projections;

    public SparkProjectionSerializer(Schema arrowSchema, EnumeratedSchema enumeratedSchema)
    {
        this.projections = new LinkedHashSet<>();
        for (Field column : arrowSchema.getFields()) {
            enumeratedSchema.collectProjectionIndices(column.getName(), ImmutableList.of(), projections::add);
        }
    }

    @Override
    protected int serialize()
    {
        int[] expressionsOffsets = projections
                .stream()
                .mapToInt(this::buildColumn)
                .toArray();
        return Project.createExpressionsVector(builder, expressionsOffsets);
    }
}