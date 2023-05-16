/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
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
import org.apache.arrow.flatbuf.Precision;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
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

    private TypeUtils()
    {
    }

    public static boolean isRowId(Field field)
    {
        return field.equals(ROW_ID_FIELD);
    }

    public static Type convertArrowFieldToTrinoType(Field field)
    {
        if (isRowId(field)) {
            return BIGINT;
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
                return TimestampType.createTimestampType(timeUnitToPrecision(timestampType.getUnit()));
            case Time:
                ArrowType.Time timeType = (ArrowType.Time) arrowType;
                return TimeType.createTimeType(timeUnitToPrecision(timeType.getUnit()));
            case Binary:
                return VARBINARY;
            case FixedSizeBinary:
                // Works for ASCII translations only
                return CharType.createCharType(((ArrowType.FixedSizeBinary) arrowType).getByteWidth());
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
                Type itemType = convertArrowFieldToTrinoType(field.getChildren().get(0));
                return new ArrayType(itemType);
            case Map:
                verify(field.getChildren().size() == 1, "unexpected number of %s children: %s", field, field.getChildren());
                Field entries = field.getChildren().get(0);
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
        switch (timeUnit) {
            case SECOND:
                return 0;
            case MILLISECOND:
                return 3;
            case MICROSECOND:
                return 6;
            case NANOSECOND:
                return 9;
            default:
                throw new TrinoException(NOT_SUPPORTED, format("Unsupported Arrow type time unit: %s", timeUnit));
        }
    }

    public static long timeUnitToPicos(TimeUnit timeUnit)
    {
        switch (timeUnit) {
            case SECOND:
                return 1_000_000_000_000L;
            case MILLISECOND:
                return 1_000_000_000L;
            case MICROSECOND:
                return 1_000_000L;
            case NANOSECOND:
                return 1_000L;
            default:
                throw new TrinoException(NOT_SUPPORTED, format("Unsupported Arrow type time unit: %s", timeUnit));
        }
    }

    public static long convertTwoValuesNanoToLong(long micros, int picos)
    {
        long result = (micros * 1000) + (picos / 1000);
        Object[] values = convertLongNanoToTwoValues(result);
        if (values[0].equals(micros) && values[1].equals(picos)) {
            return result;
        }
        throw new TrinoException(NOT_SUPPORTED, format("Unsupported timestamp value in Arrow: micros=%s<>%s, picos=%s<>%s", micros, values[0], picos, values[1]));
    }

    public static Object[] convertLongNanoToTwoValues(long nano)
    {
        long micros = Math.floorDiv(nano, 1000);
        int picos = Math.floorMod(nano, 1000) * 1000;
        return new Object[] {micros, picos};
    }

    public static TimeUnit precisionToTimeUnit(int precision)
    {
        switch (precision) {
            case 0:
                return TimeUnit.SECOND;
            case 3:
                return TimeUnit.MILLISECOND;
            case 6:
                return TimeUnit.MICROSECOND;
            case 9:
                return TimeUnit.NANOSECOND;
            default:
                throw new TrinoException(NOT_SUPPORTED, format("Unsupported precision for Trino type: %d", precision));
        }
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
        else if (trinoType instanceof VarcharType) {
            arrowType = ArrowType.Utf8.INSTANCE;
        }
        else if (trinoType instanceof CharType) {
            arrowType = new ArrowType.FixedSizeBinary(((CharType) trinoType).getLength());
        }
        else if (VARBINARY.equals(trinoType)) {
            arrowType = ArrowType.Binary.INSTANCE;
        }
        else if (DATE.equals(trinoType)) {
            arrowType = new ArrowType.Date(DAY);
        }
        else if (trinoType instanceof TimestampType) {
            TimestampType ts = (TimestampType) trinoType;
            TimeUnit timeUnit = precisionToTimeUnit(ts.getPrecision());
            arrowType = new ArrowType.Timestamp(timeUnit, null);
        }
        else if (trinoType instanceof TimeType) {
            TimeType ts = (TimeType) trinoType;
            int precision = ts.getPrecision();
            TimeUnit timeUnit = precisionToTimeUnit(precision);
            int bits = precision > 3 ? 64 : 32;
            arrowType = new ArrowType.Time(timeUnit, bits);
        }
        else if (trinoType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) trinoType;
            arrowType = ArrowType.Decimal.createDecimal(decimalType.getPrecision(), decimalType.getScale(), 128);
        }
        else if (trinoType instanceof ArrayType) {
            arrowType = ArrowType.List.INSTANCE;
            ArrayType arrayType = (ArrayType) trinoType;
            children = List.of(
                    convertTrinoTypeToArrowField(arrayType.getElementType(), ARRAY_ITEM_COLUMN_NAME, true /*nullable*/));
        }
        else if (trinoType instanceof MapType) {
            // For details, see https://github.com/apache/arrow/blob/apache-arrow-7.0.0/format/Schema.fbs#L103-L131
            arrowType = new ArrowType.Map(false /*keySorted*/);
            MapType mapType = (MapType) trinoType;
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

    // keeping unused method - useful for debugging
    public static void printVector(FieldVector vector, Optional<String> printPrefix)
    {
        if (LOG.isDebugEnabled()) {
            String prefixToPrint = printPrefix.map(s -> s + " ").orElse("");
            LOG.debug("%svector: %s, of type %s", prefixToPrint, vector, vector.getField().getType());
            LOG.debug("%svector null vector: %s", prefixToPrint, IntStream.range(0, vector.getValueCount()).boxed().map(vector::isNull).collect(Collectors.toList()));
            vector.getChildrenFromFields().forEach(child -> printVector(child, printPrefix));
        }
    }

    public static void printBlock(Block block, Optional<String> printPrefix)
    {
        if (LOG.isDebugEnabled()) {
            String prefixToPrint = printPrefix.map(s -> s + " ").orElse("");
            LOG.debug("%sblock: %s, of type %s", prefixToPrint, block, block.getClass());
            LOG.debug("%sblock null vector: %s", prefixToPrint, IntStream.range(0, block.getPositionCount()).boxed().map(block::isNull).collect(Collectors.toList()));
            block.getChildren().forEach(child -> printBlock(child, printPrefix));
        }
    }
}
