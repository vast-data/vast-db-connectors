package com.vastdata.client.rowid;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.function.Supplier;

interface RowIDStrategy extends Supplier<Field>
{
}
