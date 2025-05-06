package com.vastdata.client.rowid;

import org.apache.arrow.vector.types.pojo.Field;

import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_UINT64_FIELD;

class Int64RowIDStrategy
        implements RowIDStrategy
{
    Int64RowIDStrategy() {}

    @Override
    public Field get()
    {
        return ROW_ID_UINT64_FIELD;
    }
}
