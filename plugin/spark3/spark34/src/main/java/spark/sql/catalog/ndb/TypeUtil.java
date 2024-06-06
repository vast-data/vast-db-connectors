/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.execution.arrow.ArrayWriter;
import org.apache.spark.sql.execution.arrow.ArrowFieldWriter;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.execution.arrow.BinaryWriter;
import org.apache.spark.sql.execution.arrow.BooleanWriter;
import org.apache.spark.sql.execution.arrow.ByteWriter;
import org.apache.spark.sql.execution.arrow.DateWriter;
import org.apache.spark.sql.execution.arrow.DecimalWriter;
import org.apache.spark.sql.execution.arrow.DoubleWriter;
import org.apache.spark.sql.execution.arrow.FloatWriter;
import org.apache.spark.sql.execution.arrow.IntegerWriter;
import org.apache.spark.sql.execution.arrow.LongWriter;
import org.apache.spark.sql.execution.arrow.MapWriter;
import org.apache.spark.sql.execution.arrow.ShortWriter;
import org.apache.spark.sql.execution.arrow.StringWriter;
import org.apache.spark.sql.execution.arrow.StructWriter;
import org.apache.spark.sql.execution.arrow.TimestampWriter;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VarcharType;
import org.apache.spark.sql.util.ArrowUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.OFFSET_WIDTH;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static org.apache.spark.sql.types.DataTypes.createStructField;

public final class TypeUtil
{
    public static final String NDB_CATALOG_DOES_NOT_SUPPORT_NON_NULL_COLUMNS = "NDB catalog does not support Non null columns";
    public static final String NDB_CATALOG_DOES_NOT_SUPPORT_TYPES = "NDB catalog does not support the following types:";

    // possible types from ArrowUtils.toArrowType that are unsupported by vast
    private static final Set<ArrowType.ArrowTypeID> unsupportedNDBTypes = ImmutableSet.of(ArrowType.ArrowTypeID.Interval,
            ArrowType.ArrowTypeID.Duration,
            ArrowType.ArrowTypeID.Null);
    public static final String TIMESTAMP_PRECISION = "TIMESTAMP_PRECISION";

    private TypeUtil() {}

    public static List<Field> adaptVerifiedSparkSchemaToArrowFieldsList(StructType schema)
    {
        List<Field> fieldList = sparkSchemaToArrowFieldsList(schema);
        if (fieldList.stream().anyMatch(field -> !field.isNullable())) {
            throw new UnsupportedOperationException(NDB_CATALOG_DOES_NOT_SUPPORT_NON_NULL_COLUMNS);
        }


        List<Field> unsupportedFields = fieldList.stream().filter(field -> unsupportedNDBTypes.contains(field.getType().getTypeID())).collect(Collectors.toList());
        if (!unsupportedFields.isEmpty()) {
            throw new UnsupportedOperationException(format("%s %s", NDB_CATALOG_DOES_NOT_SUPPORT_TYPES, unsupportedFields));
        }
        return fieldList;
    }

    public static List<Field> sparkSchemaToArrowFieldsList(StructType schema)
    {
        return Arrays.stream(schema.fields())
                .map(structField -> sparkFieldToArrowField(structField.name(), structField.dataType(), structField.nullable(), structField.metadata()))
                .collect(Collectors.toList());
    }

    public static StructType arrowFieldsListToSparkSchema(List<Field> fieldList)
    {
        StructField[] fields = new StructField[fieldList.size()];
        int i = 0;
        for (Field field : fieldList) {
            fields[i++] = arrowFieldToSparkField(field);
        }
        return new StructType(fields);
    }

    public static StructField arrowFieldToSparkField(Field arrowField)
    {
        DataType dataType;
        try {
            if (arrowField.getType().getTypeID() == ArrowType.ArrowTypeID.Timestamp) {
                MetadataBuilder metadataBuilder = new MetadataBuilder();
                ArrowType.Timestamp type = (ArrowType.Timestamp) arrowField.getType();
                TimeUnit unit = type.getUnit();
                switch (unit) {
                    case SECOND:
                        metadataBuilder.putLong(TIMESTAMP_PRECISION, TimestampPrecision.SECONDS.getPrecisionID());
                        break;
                    case MILLISECOND:
                        metadataBuilder.putLong(TIMESTAMP_PRECISION, TimestampPrecision.MILLISECONDS.getPrecisionID());
                        break;
                    case MICROSECOND:
                        metadataBuilder.putLong(TIMESTAMP_PRECISION, TimestampPrecision.MICROSECONDS.getPrecisionID());
                        break;
                    case NANOSECOND:
                        metadataBuilder.putLong(TIMESTAMP_PRECISION, TimestampPrecision.NANOSECONDS.getPrecisionID());
                        break;
                }
                return createStructField(arrowField.getName(), TimestampType, arrowField.isNullable(), metadataBuilder.build());
            }
            dataType = ArrowUtils.fromArrowField(arrowField);
        }
        catch (Exception exception) {
            if (arrowField.getType().getTypeID() == ArrowType.ArrowTypeID.FixedSizeBinary) {
                dataType = new CharType(((ArrowType.FixedSizeBinary) arrowField.getType()).getByteWidth());
            } else {
                throw exception;
            }
        }
        return createStructField(arrowField.getName(), dataType, arrowField.isNullable());
    }

    public static Field sparkFieldToArrowField(StructField structField)
    {
        return sparkFieldToArrowField(structField.name(), structField.dataType(), structField.nullable(), structField.metadata());
    }

    public static Field sparkFieldToArrowField(String name, DataType sparkType, boolean nullable, Metadata metadata)
    {
        try {
            if (sparkType.equals(DataTypes.TimestampType)) {
                if (metadata.contains(TIMESTAMP_PRECISION)) {
                    long aLong = metadata.getLong(TIMESTAMP_PRECISION);
                    TimestampPrecision timestampPrecision = TimestampPrecision.fromID((int) aLong);
                    if (timestampPrecision == null) {
                        throw new RuntimeException(format("Unexpected prevision for type: %s, with metadata: %s",
                                sparkType, metadata));
                    }
                    ArrowType arrowType;
                    switch (timestampPrecision) {
                        case SECONDS:
                            arrowType = new ArrowType.Timestamp(TimeUnit.SECOND, "UTC");
                            break;
                        case MILLISECONDS:
                            arrowType = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
                            break;
                        case MICROSECONDS:
                            arrowType = new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
                            break;
                        case NANOSECONDS:
                            arrowType = new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC");
                            break;
                        default:
                            throw new RuntimeException(format("Unexpected timestamp type precision: %s for type: %s",
                                    timestampPrecision, sparkType));
                    }
                    FieldType fieldType = new FieldType(nullable, arrowType, null);
                    return new Field(name, fieldType, null);
                }
            }
            return ArrowUtils.toArrowField(name, sparkType, nullable, "UTC");
        }
        catch (UnsupportedOperationException exception) {
            if (sparkType.sameType(new VarcharType(sparkType.defaultSize()))) {
                return new Field(name, new FieldType(true, ArrowType.Utf8.INSTANCE, null), null);
            }
            if (sparkType.sameType(new CharType(sparkType.defaultSize()))) {
                return new Field(name, new FieldType(true, new ArrowType.FixedSizeBinary(((CharType) sparkType).length()), null), null);
            }
            throw exception;
        }
    }

    public static ArrowWriter getArrowSchemaWriter(VectorSchemaRoot root)
    {
        ArrowFieldWriter[] writers = root.getFieldVectors().stream().map(vector -> {
            vector.allocateNew();
            return createWriter(vector);
        }).toArray(ArrowFieldWriter[]::new);
        return new ArrowWriter(root, writers);
    }

    private static ArrowFieldWriter createWriter(ValueVector vector)
    {
        Field field = vector.getField();
        ArrowType arrowType = field.getType();
        switch (arrowType.getTypeID()) {
            case Bool: {
                return new BooleanWriter((BitVector) vector);
            }
            case Int: {
                ArrowType.Int type = (ArrowType.Int) arrowType;
                if (type.getIsSigned()) {
                    switch (type.getBitWidth()) {
                        case 8:
                            return new ByteWriter((TinyIntVector) vector);
                        case 16:
                            return new ShortWriter((SmallIntVector) vector);
                        case 32:
                            return new IntegerWriter((IntVector) vector);
                        case 64:
                            return new LongWriter((BigIntVector) vector);
                        default:
                            throw new UnsupportedOperationException("Unsupported int Arrow type length: " + arrowType);
                    }
                }
                else {
                    throw new UnsupportedOperationException("Unsupported unsigned Arrow type: " + arrowType);
                }
            }
            case FloatingPoint: {
                ArrowType.FloatingPoint type = (ArrowType.FloatingPoint) arrowType;
                switch (type.getPrecision()) {
                    case SINGLE:
                        return new FloatWriter((Float4Vector) vector);
                    case DOUBLE:
                        return new DoubleWriter((Float8Vector) vector);
                    default:
                        throw new UnsupportedOperationException("Unsupported floating-point precision: " + type);
                }
            }
            case Decimal: {
                ArrowType.Decimal decType = (ArrowType.Decimal) arrowType;
                int precision = decType.getPrecision();
                int scale = decType.getScale();
                return new DecimalWriter((DecimalVector) vector, precision, scale);
            }
            case FixedSizeBinary: {
                return new CharNWriter((FixedSizeBinaryVector) vector, ((ArrowType.FixedSizeBinary)arrowType).getByteWidth());
            }
            case Utf8: {
                return new StringWriter((VarCharVector) vector);
            }
            case Binary: {
                return new BinaryWriter((VarBinaryVector) vector);
            }
            case Date: {
                return new DateWriter((DateDayVector) vector);
            }
            case Timestamp: {
                ArrowType.Timestamp tsType = (ArrowType.Timestamp) arrowType;
                TimeUnit unit = tsType.getUnit();
                switch (unit) {
                    case SECOND:
                    case MILLISECOND:
                    case NANOSECOND:
                        return new NonMicroTimestampWriter((TimeStampVector) vector);
                    case MICROSECOND:
                        return new TimestampWriter((TimeStampMicroTZVector) vector);
                }
                throw new RuntimeException("Unexpected arrow timesatmp type: %s");
            }
            case Map: {
                MapVector mapVector = (MapVector) vector;
                StructVector structVector = (StructVector) mapVector.getDataVector();
                ArrowFieldWriter keyWriter = createWriter(structVector.getChild(MapVector.KEY_NAME));
                ArrowFieldWriter valueWriter = createWriter(structVector.getChild(MapVector.VALUE_NAME));
                return new MapWriter(mapVector, structVector, keyWriter, valueWriter);
            }
            case Struct: {
                StructVector structVector = (StructVector) vector;
                ArrowFieldWriter[] childWriters = IntStream.range(0, structVector.size())
                        .mapToObj(structVector::getChildByOrdinal)
                        .map(TypeUtil::createWriter)
                        .toArray(ArrowFieldWriter[]::new);
                return new StructWriter(structVector, childWriters);
            }
            case List: {
                ListVector listVector = (ListVector) vector;
                ArrowFieldWriter childWriter = createWriter(listVector.getDataVector());
                return new ArrayWriter(listVector, childWriter);
            }
        }
        throw new IllegalArgumentException("unsupported Arrow type: " + arrowType);
    }

    public static void convertFixedSizeBinaryIntoVarchar(FixedSizeBinaryVector src, VarCharVector dst)
    {
        final int valueCount = src.getValueCount();
        final int byteWidth = src.getByteWidth();
        ArrowBuf data = src.getDataBuffer();
        ArrowBuf validity = src.getValidityBuffer();
        try (ArrowBuf offsets = dst.getAllocator().buffer((long) (valueCount + 1) * OFFSET_WIDTH)) {
            int offset = 0;
            int index = 0;
            for (int i = 0; i <= valueCount; ++i) {
                offsets.setInt(index, offset);
                index += OFFSET_WIDTH;
                offset += byteWidth;
            }
            ArrowFieldNode node = new ArrowFieldNode(valueCount, src.getNullCount());
            // See https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout for buffer order definition
            dst.loadFieldBuffers(node, ImmutableList.of(validity, offsets, data));
            dst.setValueCount(valueCount);
        }
    }

    public static void convertNonMicroTSVectorToMicroTSVector(TimeStampVector src, TimeStampMicroTZVector dst)
    {
        int valueCount = src.getValueCount();
        List<ArrowBuf> fieldBuffers = src.getFieldBuffers();
        ArrowFieldNode node = new ArrowFieldNode(valueCount, src.getNullCount());
        dst.loadFieldBuffers(node, fieldBuffers);
    }
}
