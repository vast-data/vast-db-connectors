/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestTypeUtils
{
    private static final TypeOperators typeOperators = new TypeOperators();

    private void testBiDirectionalConvert(Field field, Type trino)
    {
        assertEquals(TypeUtils.convertArrowFieldToTrinoType(field), trino);
        assertEquals(TypeUtils.convertTrinoTypeToArrowField(trino, field.getName(), true /*nullable*/), field);
    }

    private void testBiDirectionalConvert(ArrowType arrowType, Type trino)
    {
        testBiDirectionalConvert(Field.nullable("name", arrowType), trino);
    }

    @Test
    public void testConvertScalarTypes()
    {
        testBiDirectionalConvert(ArrowType.Utf8.INSTANCE, VARCHAR);
        testBiDirectionalConvert(new ArrowType.Int(8, true), TINYINT);
        testBiDirectionalConvert(new ArrowType.Int(16, true), SMALLINT);
        testBiDirectionalConvert(new ArrowType.Int(32, true), INTEGER);
        testBiDirectionalConvert(new ArrowType.Int(64, true), BIGINT);
        testBiDirectionalConvert(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), REAL);
        testBiDirectionalConvert(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), DOUBLE);
        testBiDirectionalConvert(new ArrowType.Timestamp(TimeUnit.SECOND, null), TimestampType.TIMESTAMP_SECONDS);
        testBiDirectionalConvert(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), TimestampType.TIMESTAMP_MILLIS);
        testBiDirectionalConvert(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), TimestampType.TIMESTAMP_MICROS);
        testBiDirectionalConvert(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null), TimestampType.TIMESTAMP_NANOS);
        testBiDirectionalConvert(ArrowType.Binary.INSTANCE, VARBINARY);
        testBiDirectionalConvert(new ArrowType.Date(DateUnit.DAY), DATE);
        testBiDirectionalConvert(new ArrowType.Time(TimeUnit.SECOND, 32), TimeType.TIME_SECONDS);
        testBiDirectionalConvert(new ArrowType.Time(TimeUnit.MILLISECOND, 32), TimeType.TIME_MILLIS);
        testBiDirectionalConvert(new ArrowType.Time(TimeUnit.MICROSECOND, 64), TimeType.TIME_MICROS);
        testBiDirectionalConvert(new ArrowType.Time(TimeUnit.NANOSECOND, 64), TimeType.TIME_NANOS);
        testBiDirectionalConvert(new ArrowType.Decimal(5, 2, 128), DecimalType.createDecimalType(5, 2));
    }

    @Test
    public void testConvertNestedTypes()
    {
        testBiDirectionalConvert(
                new Field("list_name", FieldType.nullable(ArrowType.List.INSTANCE), List.of(
                        Field.nullable("item", ArrowType.Utf8.INSTANCE))),
                new ArrayType(VARCHAR));

        testBiDirectionalConvert(
                new Field("list_name", FieldType.nullable(ArrowType.List.INSTANCE), List.of(
                        new Field("item", FieldType.nullable(ArrowType.List.INSTANCE), List.of(
                            Field.nullable("item", ArrowType.Utf8.INSTANCE))))),
                new ArrayType(new ArrayType(VARCHAR)));

        testBiDirectionalConvert(
                new Field("struct_name", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(
                        new Field("f1", FieldType.nullable(ArrowType.List.INSTANCE), List.of(
                                Field.nullable("item", ArrowType.Utf8.INSTANCE))),
                        Field.nullable("f2", ArrowType.Binary.INSTANCE))),
                RowType.rowType(
                        new RowType.Field(Optional.of("f1"), new ArrayType(VARCHAR)),
                        new RowType.Field(Optional.of("f2"), VARBINARY)));

        testBiDirectionalConvert(
                new Field("map_name", FieldType.nullable(new ArrowType.Map(false)), List.of(
                        new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE), List.of(
                            Field.notNullable("key", ArrowType.Utf8.INSTANCE),
                            new Field("value", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(
                                    Field.nullable("f", ArrowType.Binary.INSTANCE))))))),
                new MapType(VARCHAR, RowType.rowType(new RowType.Field(Optional.of("f"), VARBINARY)), typeOperators));
    }

    @Test
    public void testConvertRowId()
    {
        Type type = TypeUtils.convertArrowFieldToTrinoType(ROW_ID_FIELD);
        assertEquals(type, BIGINT); // a special case for '$row_id' uint64 special column
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testExceptionForUnsupportedArrowTypeConversion()
    {
        ArrowType.FloatingPoint type = new ArrowType.FloatingPoint(FloatingPointPrecision.HALF);
        TypeUtils.convertArrowFieldToTrinoType(Field.nullable("dummy_name", type));
    }

    @Test(expectedExceptions = TrinoException.class, expectedExceptionsMessageRegExp = "Unsupported Trino type: uuid")
    public void testExceptionForUnsupportedUuid()
    {
        TypeUtils.convertTrinoTypeToArrowField(UuidType.UUID, "name", true /*nullable*/);
    }

    @Test(expectedExceptions = TrinoException.class, expectedExceptionsMessageRegExp = "Row fields must be explicitly named: row\\(varchar\\)")
    public void testExceptionForUnsupportedAnonymousStruct()
    {
        TypeUtils.convertTrinoTypeToArrowField(RowType.anonymousRow(VARCHAR), "name", true /*nullable*/);
    }

    @DataProvider
    public Object[][] supportedTimestamps()
    {
        return new Object[][] {
                {123L, 0, 123000L},
                {-123L, 0, -123000L},
                {123456L, 0, 123456000L},
                {-123456L, 0, -123456000L},
                {123456789L, 0, 123456789000L},
                {-123456789L, 0, -123456789000L},
                {Long.MAX_VALUE / 1000, 807000, Long.MAX_VALUE},
                {Long.MAX_VALUE / 1000, 806000, Long.MAX_VALUE - 1},
                {Long.MIN_VALUE / 1000, 0, Long.MIN_VALUE + 808},
                {Long.MIN_VALUE / 1000, 1000, Long.MIN_VALUE + 809},
                {123L, 456000, 123456L},
                {-123L, 456000, -122544L},
                {123L, 999000, 123999L},
                {-123L, 999000, -122001L}};
    }

    @Test(dataProvider = "supportedTimestamps")
    public void testNanoTimestampConversion(long micros, int picos, long expected)
    {
        assertEquals(TypeUtils.convertTwoValuesNanoToLong(micros, picos), expected);
    }

    @DataProvider
    public Object[][] unsupportedTimestamps()
    {
        return new Object[][] {
                {Long.MAX_VALUE / 999, 0},
                {Long.MAX_VALUE / 2, 0},
                {Long.MAX_VALUE, 0},
                {Long.MAX_VALUE / 1000, 808000}, // results in (Long.MAX_VALUE + 1) nanos
                {Long.MIN_VALUE / 999, 0},
                {Long.MIN_VALUE / 2, 0},
                {Long.MIN_VALUE, 0},
                {123L, -456000}, // no support for negative picoseconds (must be between 0 and 999999)
                {123L, 456}}; // Arrow doesn't support picosecond resolution
    }

    @Test(expectedExceptions = TrinoException.class, dataProvider = "unsupportedTimestamps")
    public void testNanoTimestampConversionUnsupported(long micros, int nanos)
    {
        TypeUtils.convertTwoValuesNanoToLong(micros, nanos);
    }
}
