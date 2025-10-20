/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.collect.ImmutableList;
import com.vastdata.client.schema.EnumeratedSchema;
import io.trino.spi.type.ArrayType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestVastPageSourceProvider
{
    private static final Field BIGINT_FIELD = TypeUtils.convertTrinoTypeToArrowField(BIGINT, "bigint", true);
    private static final Field BOOL_FIELD = new Field("bool", new FieldType(true, ArrowType.Bool.INSTANCE, null), List.of());
    private static final Field VARCHAR_FIELD = new Field("varchar", new FieldType(true, ArrowType.Utf8.INSTANCE, null), List.of());
    private static final Field ARRAY_FIELD_VARCHAR = TypeUtils.convertTrinoTypeToArrowField(new ArrayType(VARCHAR), "list1", true);
    private static final Field ARRAY_FIELD_SMALLINT = TypeUtils.convertTrinoTypeToArrowField(new ArrayType(SMALLINT), "list2", true);
    private static final Field ARRAY_FIELD_TINYINT = TypeUtils.convertTrinoTypeToArrowField(new ArrayType(TINYINT), "list3", true);
    private static final Field ROW_WITH_PLAIN_FIELDS = new Field("row", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(BOOL_FIELD, VARCHAR_FIELD));
    private static final Field ROW_WITH_ROW_AND_PLAIN_FIELDS = new Field("row_in_row", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(ROW_WITH_PLAIN_FIELDS, BOOL_FIELD, VARCHAR_FIELD));
    private static final Field ROW_WITH_PLAIN_FIELD_AND_ARRAY = new Field("complex_row_with_array", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(BOOL_FIELD, ARRAY_FIELD_SMALLINT));
    private static final Field ROW_WITH_ROW_AND_PLAIN_FIELD_AND_ARRAY = new Field("complex_row_with_array", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(ROW_WITH_PLAIN_FIELD_AND_ARRAY, BIGINT_FIELD, ARRAY_FIELD_TINYINT));

    @Test(dataProvider = "fieldsProjectionsProvider")
    public void testBuildProjectionsArray(List<Field> projectedFields, List<Integer> expectedProjections)
    {
        EnumeratedSchema enumeratedSchema = new EnumeratedSchema(projectedFields);
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (Field field : projectedFields) {
            enumeratedSchema.collectProjectionIndices(field.getName(), List.of(), builder::add);
        }
        assertEquals(builder.build(), expectedProjections);
    }

    @DataProvider
    public Object[][] fieldsProjectionsProvider()
    {
        return new Object[][] {
                {List.of(), List.of()},
                {List.of(BOOL_FIELD), List.of(0)},
                {List.of(BOOL_FIELD, VARCHAR_FIELD), List.of(0, 1)},
                {List.of(ROW_WITH_PLAIN_FIELDS), List.of(1, 2)},
                {List.of(BOOL_FIELD, ROW_WITH_PLAIN_FIELDS, VARCHAR_FIELD), List.of(0, 2, 3, 4)},
                {List.of(BOOL_FIELD, ROW_WITH_PLAIN_FIELDS, VARCHAR_FIELD, ROW_WITH_ROW_AND_PLAIN_FIELDS), List.of(0, 2, 3, 4, 7, 8, 9, 10)},
                {List.of(ARRAY_FIELD_VARCHAR, ROW_WITH_ROW_AND_PLAIN_FIELD_AND_ARRAY, ROW_WITH_ROW_AND_PLAIN_FIELDS), List.of(1, 4, 6, 7, 9, 12, 13, 14, 15)},
        };
    }
}
