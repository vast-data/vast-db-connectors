/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.vastdata.trino.rowid.ArrowTypeToTrinoRowIDTypeFunction;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.flatbuf.Precision;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD_NAME;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.arrow.vector.types.DateUnit.DAY;

public final class TypeUtils
{
    private static final Logger LOG = Logger.get(TypeUtils.class);
    public static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    // Arrow-specific column names for representing nested data types
    private static final String ARRAY_ITEM_COLUMN_NAME = "item";
    private static final String MAP_KEY_COLUMN_NAME = "key";
    private static final String MAP_VALUE_COLUMN_NAME = "value";
    private static final String MAP_ENTRIES_COLUMN_NAME = "entries";

    public record Pair<T, U>(T first, U second) { }

    private TypeUtils()
    {
    }

    public static boolean isRowId(Field field)
    {
        return field.getName().equals(ROW_ID_FIELD_NAME);
    }

    public static Type convertArrowFieldToTrinoType(Field field)
    {
        if (isRowId(field)) {
            return ArrowTypeToTrinoRowIDTypeFunction.INSTANCE.apply(field.getType());
        }
        ArrowType arrowType = field.getType();
        switch (arrowType.getTypeID()) {
            case Int: {
                ArrowType.Int type = (ArrowType.Int) arrowType;
                if (type.getIsSigned()) {
                    switch (type.getBitWidth()) {
                        case 8: return TINYINT;
                        case 16: return SMALLINT;
                        case 32: return INTEGER;
                        case 64: return BIGINT;
                    }
                }
                break;
            }
            case FloatingPoint: {
                ArrowType.FloatingPoint type = (ArrowType.FloatingPoint) arrowType;
                switch (type.getPrecision().getFlatbufID()) {
                    case Precision.SINGLE: return REAL;
                    case Precision.DOUBLE: return DOUBLE;
                }
                break;
            }
            case Bool:
                return BOOLEAN;
            case Utf8:
                return VARCHAR;
            case Timestamp:
                ArrowType.Timestamp timestampType = (ArrowType.Timestamp) arrowType;
                if (timestampType.getTimezone() == null) {
                    return TimestampType.createTimestampType(timeUnitToPrecision(timestampType.getUnit()));
                } 
                else {
                    return TimestampWithTimeZoneType.createTimestampWithTimeZoneType(timeUnitToPrecision(timestampType.getUnit()));
                }
            case Time:
                ArrowType.Time timeType = (ArrowType.Time) arrowType;
                return TimeType.createTimeType(timeUnitToPrecision(timeType.getUnit()));
            case Binary:
                return VARBINARY;
            case FixedSizeBinary: {
                final int width = ((ArrowType.FixedSizeBinary) arrowType).getByteWidth();
                if (width == 16) {
                    Map<String,String> metadata = field.getMetadata();
                    if (metadata != null && metadata.getOrDefault("ARROW:extension:name", "").equals("arrow.uuid")) {
                        return UUID;
                    }
                }
                // Works for ASCII translations only
                return CharType.createCharType(width);
            }
            case Date:
                ArrowType.Date dateType = (ArrowType.Date) arrowType;
                if (dateType.getUnit() == DAY) {
                    return DATE;
                }
            case Decimal:
                ArrowType.Decimal decType = (ArrowType.Decimal) arrowType;
                int precision = decType.getPrecision();
                int scale = decType.getScale();
                return DecimalType.createDecimalType(precision, scale);
            case Struct:
                return RowType.from(field.getChildren()
                        .stream()
                        .map(child -> RowType.field(child.getName(), convertArrowFieldToTrinoType(child)))
                        .collect(Collectors.toList()));
            case List:
                verify(field.getChildren().size() == 1, "unexpected number of %s children: %s", field, field.getChildren());
                Type itemType = convertArrowFieldToTrinoType(field.getChildren().getFirst());
                return new ArrayType(itemType);
            case Map:
                verify(field.getChildren().size() == 1, "unexpected number of %s children: %s", field, field.getChildren());
                Field entries = field.getChildren().getFirst();
                verify(entries.getType().getTypeID() == ArrowType.ArrowTypeID.Struct, "unexpected entries type: %s", entries);
                verify(entries.getChildren().size() == 2, "unexpected number of %s children: %s", entries, entries.getChildren());
                Type keyType = convertArrowFieldToTrinoType(entries.getChildren().get(0));
                Type valueType = convertArrowFieldToTrinoType(entries.getChildren().get(1));
                return new MapType(keyType, valueType, TYPE_OPERATORS);
        }
        // TODO: better exception
        throw new IllegalArgumentException("unsupported Arrow type: " + arrowType);
    }

    private static int timeUnitToPrecision(TimeUnit timeUnit)
    {
        return switch (timeUnit) {
            case SECOND -> 0;
            case MILLISECOND -> 3;
            case MICROSECOND -> 6;
            case NANOSECOND -> 9;
        };
    }

    public static long timeUnitToPicos(TimeUnit timeUnit)
    {
        return switch (timeUnit) {
            case SECOND -> 1_000_000_000_000L;
            case MILLISECOND -> 1_000_000_000L;
            case MICROSECOND -> 1_000_000L;
            case NANOSECOND -> 1_000L;
        };
    }

    public static long convertTwoValuesNanoToLong(long micros, int picos)
    {
        long result = (micros * 1000) + (picos / 1000);
        Pair<Long, Integer> values = convertLongNanoToTwoValues(result);
        if (values.first().equals(micros) && values.second().equals(picos)) {
            return result;
        }
        throw new TrinoException(NOT_SUPPORTED, format("Unsupported timestamp value in Arrow: micros=%s<>%s, picos=%s<>%s", micros, values.first(), picos, values.second()));
    }

    public static long convertTwoValuesNanoToLongMilli(long millis, int picos)
    {
        long result = (millis * 1000_000) + (picos / 1000);
        long millis2 = Math.floorDiv(result, 1000_000);
        int picos2 = Math.floorMod(result, 1000_000) * 1000;
        LOG.debug("nanos %s <- millis %s, picos %s", result, millis, picos);
        if (millis == millis2 && picos == picos2) {
            return result;
        }
        throw new TrinoException(NOT_SUPPORTED, format("Unsupported timestamp value in Arrow: micros=%s<>%s, picos=%s<>%s", millis, millis2, picos, picos2));
    }

    public static Pair<Long, Integer> convertLongNanoToTwoValues(long nano)
    {
        long micros = Math.floorDiv(nano, 1000);
        int picos = Math.floorMod(nano, 1000) * 1000;
        return new Pair<Long, Integer>(micros, picos);
    }

    public static Pair<Long, Integer> convertLongNanoToTwoValuesZone(long nano, TimeZoneKey zoneKey)
    {
        long millis = Math.floorDiv(nano, 1000_000);
        int picos = Math.floorMod(nano, 1000_000) * 1000;
        LOG.debug("nanos %s -> millis %s, picos %s", nano, millis, picos);
        return new Pair<Long, Integer>(packDateTimeWithZone(millis, zoneKey), picos);
    }

    public static TimeUnit precisionToTimeUnit(int precision)
    {
        return switch (precision) {
            case 0 -> TimeUnit.SECOND;
            case 3 -> TimeUnit.MILLISECOND;
            case 6 -> TimeUnit.MICROSECOND;
            case 9 -> TimeUnit.NANOSECOND;
            default -> throw new TrinoException(NOT_SUPPORTED, format("Unsupported precision for Trino type: %d", precision));
        };
    }

    private static TypeArg parseTrinoTypeId(final String typeId)
    {
        final String cellName;
        final int typeBegin;
        if ('"' == typeId.charAt(0)) {
            int nameEnd = 1;
            while ('"' != typeId.charAt(nameEnd)) {
                ++nameEnd;
            }
            cellName = typeId.substring(1, nameEnd);
            if (' ' != typeId.charAt(nameEnd + 1)) {
                throw new IllegalStateException(format("expected ' ' after cell name but found '%s'", typeId.charAt(nameEnd + 1)));
            }
            typeBegin = nameEnd + 2;
        }
        else {
            cellName = null;
            typeBegin = 0;
        }
        int depth = 0;
        int lastDelimiter = -1;
        final List<String> args = new ArrayList<>();
        String currentTypeName = null;
        for (int i = typeBegin; i < typeId.length(); ++i) {
            final char c = typeId.charAt(i);
            if ('(' == c) {
                ++depth;
                if (1 == depth) {
                    lastDelimiter = i;
                    if (null != currentTypeName) {
                        throw new IllegalStateException(format("currentTypeName expected to be null but currentTypeName = %s", currentTypeName));
                    }
                    currentTypeName = typeId.substring(typeBegin, i);
                }
            }
            else if (')' == c) {
                --depth;
                if (0 == depth) {
                    args.add(typeId.substring(lastDelimiter + 1, i));
                }
            }
            else if (1 == depth && ',' == c) {
                args.add(typeId.substring(lastDelimiter + 1, i));
                lastDelimiter = i;
            }
        }
        if (0 != depth) {
            throw new IllegalStateException(format("depth expected to be 0 but depth = %d", depth));
        }
        if (null == currentTypeName) {
            if (-1 != lastDelimiter) {
                throw new IllegalStateException(format("lastDelimitier expected to be -1 but lastDelimiter = %d", lastDelimiter));
            }
            currentTypeName = typeId.substring(typeBegin);
        }

        final Type currentType = switch (currentTypeName) {
            case "boolean" -> BOOLEAN;
            case "bigint" -> BIGINT;
            case "integer" -> INTEGER;
            case "smallint" -> SMALLINT;
            case "tinyint" -> TINYINT;
            case "real" -> REAL;
            case "double" -> DOUBLE;
            // case "varchar" -> VARCHAR;
            // case "varchar(n)" -> VarcharType.createVarcharType(Integer.parseInt(args.getFirst()));
            case "varchar" -> args.isEmpty() ? VARCHAR : VarcharType.createVarcharType(Integer.parseInt(args.getFirst()));
            // case "char(n)" -> CharType.createCharType(Integer.parseInt(args.getFirst()));
            case "char" -> CharType.createCharType(Integer.parseInt(args.getFirst()));
            case "varbinary" -> VARBINARY;
            case "date" -> DATE;
            // case "timestamp(n)" -> TimestampType.createTimestampType(Integer.parseInt(args.getFirst()));
            case "timestamp" -> TimestampType.createTimestampType(Integer.parseInt(args.getFirst()));
            // case "time(n)" -> TimeType.createTimeType(Integer.parseInt(args.getFirst()));
            case "time with time zone" -> TimestampWithTimeZoneType.createTimestampWithTimeZoneType(Integer.parseInt(args.getFirst()));
            case "time" -> TimeType.createTimeType(Integer.parseInt(args.getFirst()));
            // case "decimal(n,m)" -> DecimalType.createDecimalType(Integer.parseInt(args.getFirst()), Integer.parseInt(args[1]));
            case "decimal" -> DecimalType.createDecimalType(Integer.parseInt(args.getFirst()), Integer.parseInt(args.get(1)));
            // case "array(...)" -> new ArrayType(parseTrinoTypeId(args.getFirst()));
            case "array" -> new ArrayType(parseTrinoTypeId(args.getFirst()).type);
            // case "map(...)" -> new MapType(parseTrinoTypeId(args.getFirst()), parseTrinoTypeId(args[1]), new TypeOperators());
            case "map" -> new MapType(parseTrinoTypeId(args.getFirst()).type, parseTrinoTypeId(args.get(1)).type, new TypeOperators());
            // case "row(...)" -> ...
            case "row" -> RowType.from(args.stream().map(TypeUtils::parseTrinoTypeId).map(arg ->
                arg.name == null ? RowType.field(arg.type) : RowType.field(arg.name, arg.type)
            ).toList());
            default -> throw new IllegalArgumentException(format("Invalid parseTrinoTypeId currentTypeName: \"%s\", type id: \"%s\"", currentTypeName, typeId));
        };
        return new TypeArg(cellName, currentType);
    }

    public static Type parseTrinoTypeId(final TypeId typeId)
    {
        return parseTrinoTypeId(typeId.getId()).type;
    }

    public static Field convertTrinoTypeToArrowField(Type trinoType, String name, boolean nullable)
    {
        return convertTrinoTypeToArrowField(trinoType, name, nullable, null);
    }

    public static Field convertTrinoTypeToArrowField(Type trinoType, String name, boolean nullable, Map<String, String> properties)
    {
        requireNonNull(trinoType, "type is null");
        ArrowType arrowType = null;
        List<Field> children = null;
        if (BOOLEAN.equals(trinoType)) {
            arrowType = ArrowType.Bool.INSTANCE;
        }
        else if (BIGINT.equals(trinoType)) {
            arrowType = new ArrowType.Int(64, true);
        }
        else if (INTEGER.equals(trinoType)) {
            arrowType = new ArrowType.Int(32, true);
        }
        else if (SMALLINT.equals(trinoType)) {
            arrowType = new ArrowType.Int(16, true);
        }
        else if (TINYINT.equals(trinoType)) {
            arrowType = new ArrowType.Int(8, true);
        }
        else if (REAL.equals(trinoType)) {
            arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        }
        else if (DOUBLE.equals(trinoType)) {
            arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }
        else if (UUID.equals(trinoType)) {
            arrowType = new ArrowType.FixedSizeBinary(16);
            if (properties == null) {
                properties = Map.of("ARROW:extension:name", "arrow.uuid");
            }
            else {
                properties.put("ARROW:extension:name", "arrow.uuid");
            }
        }
        else if (trinoType instanceof VarcharType) {
            arrowType = ArrowType.Utf8.INSTANCE;
        }
        else if (trinoType instanceof CharType charType) {
            arrowType = new ArrowType.FixedSizeBinary(charType.getLength());
        }
        else if (VARBINARY.equals(trinoType)) {
            arrowType = ArrowType.Binary.INSTANCE;
        }
        else if (DATE.equals(trinoType)) {
            arrowType = new ArrowType.Date(DAY);
        }
        else if (trinoType instanceof TimestampType ts) {
            TimeUnit timeUnit = precisionToTimeUnit(ts.getPrecision());
            arrowType = new ArrowType.Timestamp(timeUnit, null);
        }
        else if (trinoType instanceof TimestampWithTimeZoneType ts) {
            final TimeUnit timeUnit = precisionToTimeUnit(ts.getPrecision());
            arrowType = new ArrowType.Timestamp(timeUnit, "UTC");
        }
        else if (trinoType instanceof TimeType ts) {
            int precision = ts.getPrecision();
            TimeUnit timeUnit = precisionToTimeUnit(precision);
            int bits = precision > 3 ? 64 : 32;
            arrowType = new ArrowType.Time(timeUnit, bits);
        }
        else if (trinoType instanceof DecimalType decimalType) {
            arrowType = ArrowType.Decimal.createDecimal(decimalType.getPrecision(), decimalType.getScale(), 128);
        }
        else if (trinoType instanceof ArrayType arrayType) {
            arrowType = ArrowType.List.INSTANCE;
            children = List.of(
                    convertTrinoTypeToArrowField(arrayType.getElementType(), ARRAY_ITEM_COLUMN_NAME, true /*nullable*/));
        }
        else if (trinoType instanceof MapType mapType) {
            // For details, see https://github.com/apache/arrow/blob/apache-arrow-7.0.0/format/Schema.fbs#L103-L131
            arrowType = new ArrowType.Map(false /*keySorted*/);
            Field keyField = convertTrinoTypeToArrowField(mapType.getKeyType(), MAP_KEY_COLUMN_NAME, false /*nullable*/);
            Field valueField = convertTrinoTypeToArrowField(mapType.getValueType(), MAP_VALUE_COLUMN_NAME, true /*nullable*/);
            children = List.of(
                    new Field(MAP_ENTRIES_COLUMN_NAME, FieldType.notNullable(ArrowType.Struct.INSTANCE), List.of(keyField, valueField)));
        }
        else if (trinoType instanceof RowType) {
            arrowType = ArrowType.Struct.INSTANCE;
            List<RowType.Field> fields = ((RowType) trinoType).getFields();
            children = fields
                    .stream()
                    .map(field -> {
                        String fieldName = field
                                .getName()
                                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, format("Row fields must be explicitly named: %s", trinoType)));
                        return convertTrinoTypeToArrowField(field.getType(), fieldName, true /*nullable*/);
                    })
                    .collect(Collectors.toList());
        }

        if (arrowType != null) {
            LOG.debug("Creating field '%s' with Trino type=%s to Arrow type=%s, children: %s", name, trinoType, arrowType, children);
            return new Field(name, new FieldType(nullable, arrowType, null, properties), children);
        }
        throw new TrinoException(NOT_SUPPORTED, format("Unsupported Trino type: %s", trinoType));
    }

    public static String rightPadSpaces(String value, int length)
    {
        // Works for ASCII translations only.
        // Trino does not pad values automatically, except when casting a char - Trino right-pads CHAR values: https://trino.io/docs/current/language/types.html#char
        // VAST requires padded values.
        return length > value.length() ?
            String.format("%1$-" + length + "s", value) :
            value;
    }

    private final static record TypeArg(String name, Type type) { }
}
