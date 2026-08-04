/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;

import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_UINT64_FIELD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTypeUtils
{
    private static final TypeOperators typeOperators = new TypeOperators();

    public static Object[][] supportedTimestamps()
    {
        return new Object[][] {{123L, 0, 123000L},
                               {-123L, 0, -123000L},
                               {123456L, 0, 123456000L},
                               {-123456L, 0, -123456000L},
                               {123456789L, 0, 123456789000L},
                               {-123456789L, 0, -123456789000L},
                               {Long.MAX_VALUE / 1000, 807000, Long.MAX_VALUE},
                               {Long.MAX_VALUE / 1000,
                                806000,
                                Long.MAX_VALUE - 1},
                               {Long.MIN_VALUE / 1000, 0, Long.MIN_VALUE + 808},
                               {Long.MIN_VALUE / 1000,
                                1000,
                                Long.MIN_VALUE + 809},
                               {123L, 456000, 123456L},
                               {-123L, 456000, -122544L},
                               {123L, 999000, 123999L},
                               {-123L, 999000, -122001L}};
    }

    public static Object[][] supportedFlatTrinoTypes()
    {
        return new Object[][] {{BOOLEAN},
                               {BIGINT},
                               {INTEGER},
                               {SMALLINT},
                               {TINYINT},
                               {REAL},
                               {DOUBLE},
                               {VarcharType.createVarcharType(0)},
                               {VarcharType.createVarcharType(10)},
                               {VarcharType.createVarcharType(200)},
                               {VARCHAR},
                               {CharType.createCharType(0)},
                               {CharType.createCharType(10)},
                               {CharType.createCharType(300)},
                               {DATE},
                               {TimestampType.TIMESTAMP_PICOS},
                               {TimestampType.TIMESTAMP_NANOS},
                               {TimestampType.TIMESTAMP_MILLIS},
                               {TimestampType.TIMESTAMP_SECONDS},
                               {TimestampType.createTimestampType(0)},
                               {TimestampType.createTimestampType(5)},
                               {TimestampType.createTimestampType(7)},
                               {TimestampType.createTimestampType(12)},
                               {TimeType.TIME_PICOS},
                               {TimeType.TIME_NANOS},
                               {TimeType.TIME_MILLIS},
                               {TimeType.TIME_SECONDS},
                               {TimeType.createTimeType(0)},
                               {TimeType.createTimeType(5)},
                               {TimeType.createTimeType(7)},
                               {TimeType.createTimeType(12)},
                               {DecimalType.createDecimalType()},
                               {DecimalType.createDecimalType(9, 9)},
                               {DecimalType.createDecimalType(5, 4)},
                               {DecimalType.createDecimalType(7, 5)},

                               // https://vastdata.atlassian.net/browse/ORION-230521: fix nested row types
                               {RowType.rowType(
                                       RowType.field(TimeType.TIME_PICOS))},

                               {RowType.rowType(RowType.field("bar",
                                               new MapType(BIGINT, new ArrayType(DATE),
                                                       new TypeOperators())),
                                       RowType.field(new ArrayType(
                                               RowType.rowType(RowType.field(
                                                       TimeType.TIME_PICOS)))))},

                               {RowType.rowType(RowType.field("foo",
                                       new ArrayType(RowType.rowType(
                                               RowType.field("bar",
                                                       new MapType(BIGINT,
                                                               new ArrayType(
                                                                       DATE),
                                                               new TypeOperators())),
                                               RowType.field(new ArrayType(
                                                       RowType.rowType(
                                                               RowType.field(
                                                                       TimeType.TIME_PICOS))))))))},

                               {new ArrayType(RowType.rowType(
                                       RowType.field("foo", new ArrayType(
                                               RowType.rowType(
                                                       RowType.field("bar",
                                                               new MapType(
                                                                       BIGINT,
                                                                       new ArrayType(
                                                                               DATE),
                                                                       new TypeOperators())),
                                                       RowType.field(
                                                               new ArrayType(
                                                                       RowType.rowType(
                                                                               RowType.field(
                                                                                       TimeType.TIME_PICOS)))))))))}};
    }

    public static Object[][] unsupportedTimestamps()
    {
        return new Object[][] {{Long.MAX_VALUE / 999, 0},
                               {Long.MAX_VALUE / 2, 0},
                               {Long.MAX_VALUE, 0},
                               {Long.MAX_VALUE / 1000, 808000},
                               // results in (Long.MAX_VALUE + 1) nanos
                               {Long.MIN_VALUE / 999, 0},
                               {Long.MIN_VALUE / 2, 0},
                               {Long.MIN_VALUE, 0},
                               {123L, -456000},
                               // no support for negative picoseconds (must be between 0 and 999999)
                               {123L,
                                456}}; // Arrow doesn't support picosecond resolution
    }

    private void testBiDirectionalConvert(Field field, Type trino)
    {
        assertEquals(TypeUtils.convertArrowFieldToTrinoType(field), trino);
        assertEquals(
                TypeUtils.convertTrinoTypeToArrowField(trino, field.getName(),
                        true /*nullable*/), field);
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
        testBiDirectionalConvert(
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
                REAL);
        testBiDirectionalConvert(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
                DOUBLE);
        testBiDirectionalConvert(new ArrowType.Timestamp(TimeUnit.SECOND, null),
                TimestampType.TIMESTAMP_SECONDS);
        testBiDirectionalConvert(
                new ArrowType.Timestamp(TimeUnit.MILLISECOND, null),
                TimestampType.TIMESTAMP_MILLIS);
        testBiDirectionalConvert(
                new ArrowType.Timestamp(TimeUnit.MICROSECOND, null),
                TimestampType.TIMESTAMP_MICROS);
        testBiDirectionalConvert(
                new ArrowType.Timestamp(TimeUnit.NANOSECOND, null),
                TimestampType.TIMESTAMP_NANOS);
        testBiDirectionalConvert(ArrowType.Binary.INSTANCE, VARBINARY);
        testBiDirectionalConvert(new ArrowType.Date(DateUnit.DAY), DATE);
        testBiDirectionalConvert(new ArrowType.Time(TimeUnit.SECOND, 32),
                TimeType.TIME_SECONDS);
        testBiDirectionalConvert(new ArrowType.Time(TimeUnit.MILLISECOND, 32),
                TimeType.TIME_MILLIS);
        testBiDirectionalConvert(new ArrowType.Time(TimeUnit.MICROSECOND, 64),
                TimeType.TIME_MICROS);
        testBiDirectionalConvert(new ArrowType.Time(TimeUnit.NANOSECOND, 64),
                TimeType.TIME_NANOS);
        testBiDirectionalConvert(new ArrowType.Decimal(5, 2, 128),
                DecimalType.createDecimalType(5, 2));
    }

    @Test
    public void testConvertNestedTypes()
    {
        testBiDirectionalConvert(new Field("list_name",
                        FieldType.nullable(ArrowType.List.INSTANCE),
                        List.of(Field.nullable("item", ArrowType.Utf8.INSTANCE))),
                new ArrayType(VARCHAR));

        testBiDirectionalConvert(new Field("list_name",
                        FieldType.nullable(ArrowType.List.INSTANCE),
                        List.of(new Field("item",
                                FieldType.nullable(ArrowType.List.INSTANCE),
                                List.of(Field.nullable("item",
                                        ArrowType.Utf8.INSTANCE))))),
                new ArrayType(new ArrayType(VARCHAR)));

        testBiDirectionalConvert(new Field("struct_name",
                        FieldType.nullable(ArrowType.Struct.INSTANCE),
                        List.of(new Field("f1",
                                        FieldType.nullable(ArrowType.List.INSTANCE),
                                        List.of(Field.nullable("item",
                                                ArrowType.Utf8.INSTANCE))),
                                Field.nullable("f2", ArrowType.Binary.INSTANCE))),
                RowType.rowType(new RowType.Field(Optional.of("f1"),
                                new ArrayType(VARCHAR)),
                        new RowType.Field(Optional.of("f2"), VARBINARY)));

        testBiDirectionalConvert(new Field("map_name",
                        FieldType.nullable(new ArrowType.Map(false)),
                        List.of(new Field("entries",
                                FieldType.notNullable(ArrowType.Struct.INSTANCE),
                                List.of(Field.notNullable("key",
                                        ArrowType.Utf8.INSTANCE), new Field("value",
                                        FieldType.nullable(ArrowType.Struct.INSTANCE),
                                        List.of(Field.nullable("f",
                                                ArrowType.Binary.INSTANCE))))))),
                new MapType(VARCHAR, RowType.rowType(
                        new RowType.Field(Optional.of("f"), VARBINARY)),
                        typeOperators));
    }

    @Test
    public void testConvertRowId()
    {
        Type type = TypeUtils.convertArrowFieldToTrinoType(ROW_ID_UINT64_FIELD);
        assertEquals(type,
                BIGINT); // a special case for '$row_id' uint64 special column
    }

    @Test
    public void testExceptionForUnsupportedArrowTypeConversion()
    {
        ArrowType.FloatingPoint type = new ArrowType.FloatingPoint(
                FloatingPointPrecision.HALF);
        assertThrows(IllegalArgumentException.class,
                () -> TypeUtils.convertArrowFieldToTrinoType(
                        Field.nullable("dummy_name", type)));
    }

    @Test
    public void testExceptionForUnsupportedAnonymousStruct()
    {
        assertThrows(TrinoException.class,
                () -> TypeUtils.convertTrinoTypeToArrowField(
                        RowType.anonymousRow(VARCHAR), "name",
                        true /*nullable*/),
                "Row fields must be explicitly named: row\\(varchar\\)");
    }

    @ParameterizedTest
    @MethodSource("supportedTimestamps")
    public void testNanoTimestampConversion(long micros,
                                            int picos,
                                            long expected)
    {
        assertEquals(TypeUtils.convertTwoValuesNanoToLong(micros, picos),
                expected);
    }

    @Test
    public void testParseTrinoTypeIdToTrinoType0()
    {
        final Type trinoType = RowType.rowType(RowType.field("foo", BIGINT),
                RowType.field("bar", DATE));
        assertEquals(TypeUtils.parseTrinoTypeId(trinoType.getTypeId()),
                trinoType);
    }

    @Test
    public void testParseTrinoTypeIdToTrinoType1()
    {
        final Type trinoType = RowType.rowType(RowType.field("foo", BIGINT),
                RowType.field("nope", RowType.rowType(
                        RowType.field("a", new ArrayType(TINYINT)))),
                RowType.field("bar", DATE));
        assertEquals(TypeUtils.parseTrinoTypeId(trinoType.getTypeId()),
                trinoType);
    }

    @Test
    public void testParseTrinoTypeIdToTrinoType2()
    {
        final Type trinoType = RowType.rowType(RowType.field("foo",
                new ArrayType(RowType.rowType(RowType.field("bar",
                        new MapType(BIGINT, new ArrayType(DATE),
                                new TypeOperators())),
                        RowType.field(new ArrayType(RowType.rowType(
                                RowType.field(TimeType.TIME_PICOS))))))));
        assertEquals(TypeUtils.parseTrinoTypeId(trinoType.getTypeId()),
                trinoType);
    }

    @ParameterizedTest
    @MethodSource("supportedFlatTrinoTypes")
    public void testParseTrinoTypeIdToTrinoTypeFlat(final Type trinoType)
    {
        assertEquals(TypeUtils.parseTrinoTypeId(trinoType.getTypeId()),
                trinoType);
    }

    @ParameterizedTest
    @MethodSource("supportedFlatTrinoTypes")
    public void testParseTrinoTypeIdToTrinoTypeArray(final Type trinoType)
    {
        final Type arrayType = new ArrayType(trinoType);
        assertEquals(TypeUtils.parseTrinoTypeId(arrayType.getTypeId()),
                arrayType);
    }

    @ParameterizedTest
    @MethodSource("supportedFlatTrinoTypes")
    public void testParseTrinoTypeIdToTrinoTypeMap(final Type trinoType)
    {
        final Type mapType = new MapType(trinoType, trinoType,
                new TypeOperators());
        assertEquals(TypeUtils.parseTrinoTypeId(mapType.getTypeId()), mapType);
    }

    @ParameterizedTest
    @MethodSource("supportedFlatTrinoTypes")
    public void testParseTrinoTypeIdToTrinoTypeRow(final Type trinoType)
    {
        final Type rowType = RowType.rowType(RowType.field(trinoType),
                RowType.field("named", trinoType),
                RowType.field("spaced name", trinoType),
                RowType.field("name with multiple spaces", trinoType));
        assertEquals(TypeUtils.parseTrinoTypeId(rowType.getTypeId()), rowType);
    }

    @ParameterizedTest
    @MethodSource("unsupportedTimestamps")
    public void testNanoTimestampConversionUnsupported(long micros, int nanos)
    {
        assertThrows(TrinoException.class,
                () -> TypeUtils.convertTwoValuesNanoToLong(micros, nanos));
    }

    /**
     * Test timestamp conversions for MILLISECOND, MICROSECOND, and NANOSECOND
     * with both historical (1600) and future (2200) dates.
     */
    @Test
    public void testTimestampWithTimeZoneConversions()
    {
        io.trino.spi.type.TimeZoneKey utcKey = io.trino.spi.type.TimeZoneKey.UTC_KEY;

        String[] dates = {"1600-02-29T12:00:00Z",
                          // Historical - would overflow with old approach
                          "2024-06-15T12:00:00Z",
                          // Current
                          "2200-01-01T00:00:00Z",
                          // Future
        };

        for (String dateStr : dates) {
            java.time.Instant instant = java.time.Instant.parse(dateStr);
            long epochSeconds = instant.getEpochSecond();

            // MILLISECOND
            long millis = java.util.concurrent.TimeUnit.SECONDS.toMillis(
                    epochSeconds);
            com.vastdata.Pair<Long, Integer> milliResult = TypeUtils.convertLongMilliToTwoValuesZone(
                    millis, utcKey);
            assertEquals(0, milliResult.getRight(),
                    dateStr + " MILLI: picosOfMilli should be 0");

            // MICROSECOND
            long micros = java.util.concurrent.TimeUnit.SECONDS.toMicros(
                    epochSeconds);
            com.vastdata.Pair<Long, Integer> microResult = TypeUtils.convertLongMicroToTwoValuesZone(
                    micros, utcKey);
            assertEquals(0, microResult.getRight() % 1_000_000,
                    dateStr + " MICRO: picosOfMilli should be aligned");

            // NANOSECOND
            long nanos = java.util.concurrent.TimeUnit.SECONDS.toNanos(
                    epochSeconds);
            com.vastdata.Pair<Long, Integer> nanoResult = TypeUtils.convertLongNanoToTwoValuesZone(
                    nanos, utcKey);
            assertEquals(0, nanoResult.getRight() % 1_000,
                    dateStr + " NANO: picosOfMilli should be aligned");
        }
    }

    /**
     * Test timestamp conversions WITHOUT timezone for NANOSECOND with
     * historical (1600), current, and future (2200) dates. Verifies that
     * convertLongNanoToTwoValues handles negative timestamps correctly.
     */
    @Test
    public void testTimestampWithoutTimeZoneConversions()
    {
        String[] dates = {"1600-02-29T12:00:00Z",
                          // Historical - large negative value
                          "2024-06-15T12:00:00Z",
                          // Current
                          "2200-01-01T00:00:00Z",
                          // Future
        };

        for (String dateStr : dates) {
            java.time.Instant instant = java.time.Instant.parse(dateStr);
            long epochSeconds = instant.getEpochSecond();

            // NANOSECOND - the only unit used for long timestamps without timezone
            long nanos = java.util.concurrent.TimeUnit.SECONDS.toNanos(
                    epochSeconds);
            com.vastdata.Pair<Long, Integer> nanoResult = TypeUtils.convertLongNanoToTwoValues(
                    nanos);

            // Verify picosOfMicro is aligned (should be 0 since input has no fractional part)
            assertEquals(0, nanoResult.getRight() % 1_000,
                    dateStr + " NANO: picosOfMicro should be aligned");

            // Verify we can reconstruct the original value
            long reconstructed = nanoResult.getLeft() * 1000 + nanoResult.getRight() / 1000;
            assertEquals(nanos, reconstructed,
                    dateStr + " NANO: should reconstruct original value");
        }
    }

    /**
     * Test round-trip conversion: date string with microseconds -> micros ->
     * Trino representation -> back. Uses the safe conversion function
     * (convertLongMicroToTwoValuesZone) that avoids overflow by using
     * floorDiv/floorMod instead of multiplication.
     */
    @Test
    public void testTimestampRoundTripWithMicroseconds()
    {
        io.trino.spi.type.TimeZoneKey utcKey = io.trino.spi.type.TimeZoneKey.UTC_KEY;

        String[] datesWithMicros = {"2024-06-15T12:30:45.123456Z",
                                    "1600-02-29T12:00:00.999999Z",
                                    // Historical with max micros - would overflow with naive multiply
                                    "2200-01-01T00:00:00.000001Z",
                                    // Future with min micros
        };

        for (String dateStr : datesWithMicros) {
            java.time.Instant instant = java.time.Instant.parse(dateStr);

            // Get epoch micros from the instant
            long epochMicros = java.util.concurrent.TimeUnit.SECONDS.toMicros(
                    instant.getEpochSecond()) + java.util.concurrent.TimeUnit.NANOSECONDS.toMicros(
                    instant.getNano());

            // Use the safe conversion function (this is what the fixed code path uses)
            com.vastdata.Pair<Long, Integer> result = TypeUtils.convertLongMicroToTwoValuesZone(
                    epochMicros, utcKey);

            // Extract millis and picos from the result
            long packedMillisWithZone = result.getLeft();
            int picosOfMilli = result.getRight();

            // Unpack the millis (remove timezone info)
            long epochMillis = io.trino.spi.type.DateTimeEncoding.unpackMillisUtc(
                    packedMillisWithZone);

            // Reconstruct micros: millis * 1000 + picosOfMilli / 1_000_000
            long reconstructedMicros = epochMillis * 1000 + picosOfMilli / 1_000_000;

            // Verify we get back original micros
            assertEquals(epochMicros, reconstructedMicros,
                    dateStr + ": micros should be preserved through conversion");

            // Verify picosOfMilli is properly aligned for microsecond precision
            assertEquals(0, picosOfMilli % 1_000_000,
                    dateStr + ": picosOfMilli should be aligned to microsecond precision");
        }
    }

    /**
     * Test that converting micros to millis loses microsecond precision (rounds
     * down). This verifies the behavior when data is stored at millisecond
     * precision.
     */
    @Test
    public void testMicrosToMillisRounding()
    {
        io.trino.spi.type.TimeZoneKey utcKey = io.trino.spi.type.TimeZoneKey.UTC_KEY;

        String[] datesWithMicros = {"2024-06-15T12:30:45.123456Z",
                                    // .123456 -> .123 (loses 456 micros)
                                    "1600-02-29T12:00:00.999999Z",
                                    // .999999 -> .999 (loses 999 micros)
                                    "2200-01-01T00:00:00.000001Z",
                                    // .000001 -> .000 (loses 1 micro)
        };

        for (String dateStr : datesWithMicros) {
            java.time.Instant instant = java.time.Instant.parse(dateStr);

            // Get epoch micros from the instant
            long epochMicros = java.util.concurrent.TimeUnit.SECONDS.toMicros(
                    instant.getEpochSecond()) + java.util.concurrent.TimeUnit.NANOSECONDS.toMicros(
                    instant.getNano());

            // Convert micros to millis (this loses sub-millisecond precision)
            long epochMillis = Math.floorDiv(epochMicros, 1000);

            // Use millisecond conversion function
            com.vastdata.Pair<Long, Integer> result = TypeUtils.convertLongMilliToTwoValuesZone(
                    epochMillis, utcKey);

            // Extract millis and picos from the result
            long packedMillisWithZone = result.getLeft();
            int picosOfMilli = result.getRight();

            // Unpack the millis (remove timezone info)
            long reconstructedMillis = io.trino.spi.type.DateTimeEncoding.unpackMillisUtc(
                    packedMillisWithZone);

            // Verify millis match
            assertEquals(epochMillis, reconstructedMillis,
                    dateStr + ": millis should be preserved");

            // Verify picosOfMilli is 0 for millisecond precision
            assertEquals(0, picosOfMilli,
                    dateStr + ": picosOfMilli should be 0 for millisecond precision");

            // Verify micros are rounded (sub-millisecond part lost)
            long expectedRoundedMicros = epochMillis * 1000;
            long lostMicros = epochMicros - expectedRoundedMicros;
            assertTrue(lostMicros >= 0 && lostMicros < 1000,
                    dateStr + ": should lose 0-999 microseconds, lost: " + lostMicros);
        }
    }
}
