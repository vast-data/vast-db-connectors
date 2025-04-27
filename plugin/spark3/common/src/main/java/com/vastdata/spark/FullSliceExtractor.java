/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.function.UnaryOperator;

public class FullSliceExtractor implements UnaryOperator<VectorSchemaRoot>
{
    private final Schema schema;
    public FullSliceExtractor(Schema schema)
    {
        this.schema = schema;
    }

    @Override
    public VectorSchemaRoot apply(VectorSchemaRoot root)
    {
        VectorSchemaRoot slice = root.slice(0, root.getRowCount());
        // ORION-114731: Arrow Java implementation replaces internal List field name to `$data$`, instead of the standard `item` name.
        return new VectorSchemaRoot(schema, slice.getFieldVectors(), slice.getRowCount());
    }
}
