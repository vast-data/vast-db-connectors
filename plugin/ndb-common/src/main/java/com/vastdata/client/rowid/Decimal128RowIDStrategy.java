package com.vastdata.client.rowid;

import org.apache.arrow.vector.types.pojo.Field;

import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_DEC128_FIELD;

class Decimal128RowIDStrategy
        implements RowIDStrategy
{
    Decimal128RowIDStrategy() {}

    @Override
    public Field get()
    {
        return ROW_ID_DEC128_FIELD;
    }
}
