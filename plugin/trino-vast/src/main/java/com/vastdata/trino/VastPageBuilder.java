/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.collect.Iterators;
import com.vastdata.client.QueryDataPageBuilder;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.ByteArrayBlockBuilder;
import io.trino.spi.block.Fixed12BlockBuilder;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.IntArrayBlockBuilder;
import io.trino.spi.block.Int128ArrayBlockBuilder;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.ShortArrayBlockBuilder;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.trimTrailingSpaces;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeType.TIME_NANOS;
import static io.trino.spi.type.TimeType.TIME_SECONDS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Float.floatToIntBits;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.apache.arrow.vector.BaseVariableWidthVector.OFFSET_WIDTH;

public class VastPageBuilder
        implements QueryDataPageBuilder<Page>
{
    private static final Logger LOG = Logger.get(VastPageBuilder.class);
    private static final int DEFAULT_BATCHES_CAPACITY = 4;

    // Mapping validity bitmap bytes into a NULL boolean array.
    private static final boolean[][] NULL_UNPACKING_TABLE;

    private static final Unsafe unsafe;

    static {
        try {
            java.lang.reflect.Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            unsafe = (Unsafe) theUnsafe.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Unable to access Unsafe", e);
        }
    }

    static {
        NULL_UNPACKING_TABLE = new boolean[256][];
        for (int row = 0; row < 256; ++row) {
            boolean[] isNull = new boolean[8];
            for (int bit = 0; bit < 8; ++bit) {
                isNull[bit] = (row & (1 << bit)) == 0;
            }
            NULL_UNPACKING_TABLE[row] = isNull;
        }
    }

    private final Schema schema;
    private final List<VectorSchemaRoot> batches;
    private final String traceStr;

    public VastPageBuilder(String traceStr, Schema schema)
    {
        this.traceStr = traceStr;
        this.schema = schema;
        this.batches = new ArrayList<>(DEFAULT_BATCHES_CAPACITY);
    }

    @Override
    public VastPageBuilder add(VectorSchemaRoot root)
    {
        batches.add(root);
        return this;
    }

    @Override
    public Page build()
    {
        int positions = Math.toIntExact(batches.stream().mapToLong(batch -> (long) batch.getRowCount()).sum());
        List<Field> fields = schema.getFields();
        List<Type> types = fields.stream().map(TypeUtils::convertArrowFieldToTrinoType).collect(Collectors.toList());
        LOG.debug("QueryData(%s) converting %s batches (total %d rows) to Trino page: %s", traceStr, batches.size(), positions, types);
        Block[] blocks = IntStream
                .range(0, types.size())
                .mapToObj(column -> {
                    // a list of vectors matching current column
                    List<FieldVector> vectors = batches
                            .stream()
                            .map(batch -> batch.getVector(column))
                            .collect(Collectors.toList());
                    return buildBlock(types.get(column), fields.get(column), positions, vectors, Optional.empty());
                })
                .toArray(Block[]::new);
        return new Page(positions, blocks);
    }

    @Override
    public void clear()
    {
        batches.forEach(VectorSchemaRoot::close);
        batches.clear();
    }

    private boolean isTimeType(Type type, int precision)
    {
        return (type instanceof TimeType timeType) && timeType.getPrecision() == precision;
    }

    private boolean isTimestampType(Type type, int precision)
    {
        return (type instanceof TimestampType timestampType) &&  timestampType.getPrecision() == precision;
    }

    private boolean isTimestampWithTimeZoneType(Type type, int precision)
    {
        return (type instanceof TimestampWithTimeZoneType timestampType) && timestampType.getPrecision() == precision;
    }

    private Optional<boolean[]> compactIsNull(Optional<boolean[]> vectorIsNull, Optional<boolean[]> parentVectorIsNull)
    {
        if (parentVectorIsNull.isEmpty()) {
            return vectorIsNull;
        }
        else {
            boolean[] parentNulls = parentVectorIsNull.orElseThrow();
            if (vectorIsNull.isEmpty()) {
                boolean[] newChildNulls = new boolean[parentNulls.length];
                int newSize = 0;
                for (int i = 0; i < newChildNulls.length; i++) {
                    if (!parentNulls[i]) {
                        newChildNulls[newSize++] = false;
                    }
                }
                return Optional.of(Arrays.copyOfRange(newChildNulls, 0, newSize));
            }
            else {
                boolean[] childNulls = vectorIsNull.orElseThrow();
                boolean[] newChildNulls = new boolean[parentNulls.length];
                int newSize = 0;
                for (int i = 0; i < childNulls.length; i++) {
                    if (!parentNulls[i]) {
                        newChildNulls[newSize++] = childNulls[i];
                    }
                    else {
                        newChildNulls[newSize++] = true;
                    }
                }
                return (newSize != childNulls.length) ?
                    Optional.of(Arrays.copyOfRange(newChildNulls, 0, newSize)) :
                    Optional.of(newChildNulls);
            }
        }
    }

    private Block buildBlock(Type type, Field field, int positions, List<FieldVector> vectors, Optional<boolean[]> parentVectorIsNull)
    {
        ArrowType arrowType = field.getType();
        // if parentVectorIsNull is present, a compacted 'compactedIsNull' vector will be present
        Optional<boolean[]> vectorIsNull = buildIsNull(positions, vectors);
        Optional<boolean[]> compactedIsNull = compactIsNull(vectorIsNull, parentVectorIsNull);
        if (type instanceof VarcharType || VARBINARY.equals(type)) {
            int totalBytes = toIntExact(vectors
                    .stream()
                    .mapToLong(vector -> vector.getDataBuffer().readableBytes())
                    .sum());
            Slice slice = Slices.allocate(totalBytes);
            copyDataBuffers(slice, vectors);
            if (parentVectorIsNull.isPresent()) {
                int[] offsets = buildOffsetsUsingParentNullsVector(vectors, parentVectorIsNull.orElseThrow());
                verify(offsets[offsets.length - 1] == totalBytes, "QueryData(%s) last offset is %s, instead of %s", traceStr, offsets[offsets.length - 1], totalBytes);
                return new VariableWidthBlock(offsets.length - 1, slice, offsets, compactedIsNull);
            }
            else {
                int[] offsets = buildOffsets(positions, vectors);
                verify(offsets[positions] == totalBytes, "QueryData(%s) last offset is %s, instead of %s", traceStr, offsets[positions], totalBytes);
                return new VariableWidthBlock(positions, slice, offsets, compactedIsNull);
            }
        }
        else if (BIGINT.equals(type) || isTimestampType(type, TIMESTAMP_MICROS.getPrecision())) {
            if (parentVectorIsNull.isPresent()) {
                BlockBuilder builder = new LongArrayBlockBuilder(null, compactedIsNull.orElseThrow().length);
                vectors.forEach(vector -> copyLong(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.orElseThrow()));
                return builder.build();
            }
            else {
                // NOTE: TIMESTAMP(6) has the same representation in Trino & Arrow
                byte[] values = new byte[SIZE_OF_LONG * positions];
                copyDataBuffers(Slices.wrappedBuffer(values), vectors);
                return new LongArrayBlock(positions, compactedIsNull, byteArrayAsLongArray(values));
            }
        }
        else if (DOUBLE.equals(type)) {
            if (parentVectorIsNull.isPresent()) {
                BlockBuilder builder = new LongArrayBlockBuilder(null, compactedIsNull.orElseThrow().length);
                vectors.forEach(vector -> copyDouble(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.orElseThrow()));
                return builder.build();
            }
            else {
                byte[] values = new byte[SIZE_OF_LONG * positions];
                copyDataBuffers(Slices.wrappedBuffer(values), vectors);
                return new LongArrayBlock(positions, compactedIsNull, byteArrayAsLongArray(values));
            }
        }
        else if (type instanceof CharType) {
            if (parentVectorIsNull.isPresent()) {
                BlockBuilder builder = type.createBlockBuilder(null, compactedIsNull.orElseThrow().length);
                vectors.forEach(vector -> copyCharsUsingParentNullsBitmap(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.orElseThrow()));
                return builder.build();
            }
            else {
                int expectedBytesPerEntry = ((CharType) type).getLength();
                BlockBuilder builder = type.createBlockBuilder(new PageBuilderStatus(positions * expectedBytesPerEntry).createBlockBuilderStatus(), positions, expectedBytesPerEntry);
                vectors.forEach(vector -> copyChars(vector.getValueCount(), vector.getReader(), builder));
                return builder.build();
            }
        }
        else if (TINYINT.equals(type)) {
            if (parentVectorIsNull.isPresent()) {
                BlockBuilder builder = TINYINT.createFixedSizeBlockBuilder(compactedIsNull.orElseThrow().length);
                vectors.forEach(vector -> copyTinyInt(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.orElseThrow()));
                return builder.build();
            }
            else {
                byte[] values = new byte[positions];
                copyDataBuffers(Slices.wrappedBuffer(values), vectors);
                return new ByteArrayBlock(positions, compactedIsNull, values);
            }
        }
        else if (SMALLINT.equals(type)) {
            if (parentVectorIsNull.isPresent()) {
                BlockBuilder builder = SMALLINT.createFixedSizeBlockBuilder(compactedIsNull.orElseThrow().length);
                vectors.forEach(vector -> copySmallInt(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.orElseThrow()));
                return builder.build();
            }
            else {
                byte[] values = new byte[SIZE_OF_SHORT * positions];
                copyDataBuffers(Slices.wrappedBuffer(values), vectors);
                return new ShortArrayBlock(positions, compactedIsNull, byteArrayAsShortArray(values));
            }
        }
        else if (INTEGER.equals(type) || DATE.equals(type)) {
            if (parentVectorIsNull.isPresent()) {
                BlockBuilder builder = new IntArrayBlockBuilder(null, compactedIsNull.orElseThrow().length);
                vectors.forEach(vector -> copyInt(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.orElseThrow()));
                return builder.build();
            }
            else {
                byte[] values = new byte[SIZE_OF_INT * positions];
                copyDataBuffers(Slices.wrappedBuffer(values), vectors);
                return new IntArrayBlock(positions, compactedIsNull, byteArrayAsIntArray(values));
            }
        }
        else if (REAL.equals(type)) {
            if (parentVectorIsNull.isPresent()) {
                BlockBuilder builder = new IntArrayBlockBuilder(null, compactedIsNull.orElseThrow().length);
                vectors.forEach(vector -> copyFloat(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.orElseThrow()));
                return builder.build();
            }
            else {
                byte[] values = new byte[SIZE_OF_INT * positions];
                copyDataBuffers(Slices.wrappedBuffer(values), vectors);
                return new IntArrayBlock(positions, compactedIsNull, byteArrayAsIntArray(values));
            }
        }
        else if (BOOLEAN.equals(type)) {
            BlockBuilder builder;
            if (parentVectorIsNull.isPresent()) {
                builder = BOOLEAN.createFixedSizeBlockBuilder(compactedIsNull.orElseThrow().length);
                vectors.forEach(vector -> copyBooleanUsingParentNullsBitmap(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.orElseThrow()));
            }
            else {
                builder = BOOLEAN.createFixedSizeBlockBuilder(positions);
                vectors.forEach(vector -> copyBoolean(vector.getValueCount(), vector.getReader(), builder));
            }
            return builder.build();
        }
        else if (type instanceof DecimalType decimalType) {
            BlockBuilder builder = decimalType.createFixedSizeBlockBuilder(positions);
            if (parentVectorIsNull.isPresent()) {
                if (decimalType.isShort()) {
                    // convert 128-bit decimal into 64-bit block
                    vectors.forEach(vector -> copyShortDecimalUsingParentNullsBitmap(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.orElseThrow()));
                }
                else {
                    vectors.forEach(vector -> copyLongDecimalUsingParentNullsBitmap(vector.getValueCount(), vector.getReader(), builder, decimalType, parentVectorIsNull.orElseThrow()));
                }
            }
            else {
                if (decimalType.isShort()) {
                    // convert 128-bit decimal into 64-bit block
                    vectors.forEach(vector -> copyShortDecimal(vector.getValueCount(), vector.getReader(), builder));
                }
                else {
                    vectors.forEach(vector -> copyLongDecimal(vector.getValueCount(), vector.getReader(), builder, decimalType));
                }
            }
            return builder.build();
        }
        else if (isTimeType(type, TIME_SECONDS.getPrecision())) {
            BlockBuilder builder = TIME_SECONDS.createFixedSizeBlockBuilder(positions);
            vectors.forEach(vector -> copyTimeSeconds(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull));
            return builder.build();
        }
        else if (isTimeType(type, TIME_MILLIS.getPrecision())) {
            BlockBuilder builder = TIME_MILLIS.createFixedSizeBlockBuilder(positions);
            // NOTE: TimeMilliReaderImpl doesn't implement readInteger(), so we use the vector instead
            vectors.forEach(vector -> copyTimeMillis(vector.getValueCount(), (TimeMilliVector) vector, builder, parentVectorIsNull));
            return builder.build();
        }
        else if (isTimeType(type, TIME_MICROS.getPrecision())) {
            BlockBuilder builder = TIME_MICROS.createFixedSizeBlockBuilder(positions);
            vectors.forEach(vector -> copyTimeMicros(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull));
            return builder.build();
        }
        else if (isTimeType(type, TIME_NANOS.getPrecision())) {
            BlockBuilder builder = TIME_NANOS.createFixedSizeBlockBuilder(positions);
            vectors.forEach(vector -> copyTimeNanos(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull));
            return builder.build();
        }
        else if (type instanceof TimestampType timestampType) {
            BlockBuilder builder = timestampType.createFixedSizeBlockBuilder(positions);
            TimeUnit unit = ((ArrowType.Timestamp) arrowType).getUnit();
            if (timestampType.isShort()) {
                // Trino represents ShortTimestampType as a long (microseconds from epoch)
                long microsInUnit = TypeUtils.timeUnitToPicos(unit) / TypeUtils.timeUnitToPicos(TimeUnit.MICROSECOND);
                verify(microsInUnit > 0, "%s is not supported for %s", unit, timestampType);
                vectors.forEach(vector -> copyShortTimestamp(vector.getValueCount(), vector.getReader(), builder, microsInUnit == 1? m -> m : m -> microsInUnit * m, parentVectorIsNull));
            }
            else {
                // Trino represents LongTimestampType as a long (microseconds from epoch) + an int (microsecond fraction, in picoseconds)
                vectors.forEach(vector -> copyTimestampNanos(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull));
            }
            return builder.build();
        }
        else if (type instanceof TimestampWithTimeZoneType timestampType) {
            BlockBuilder builder = timestampType.createFixedSizeBlockBuilder(positions);
            TimeUnit unit = ((ArrowType.Timestamp) arrowType).getUnit();
            String zoneId = ((ArrowType.Timestamp) arrowType).getTimezone();
            if (timestampType.isShort()) {
                // Trino represents ShortTimestampType as a long (milliseconds from epoch)
                long millisInUnit = TypeUtils.timeUnitToPicos(unit) / TypeUtils.timeUnitToPicos(TimeUnit.MILLISECOND);
                verify(millisInUnit > 0, "%s is not supported for %s", unit, timestampType);
                vectors.forEach(vector -> copyShortTimestampWithTimezone(vector.getValueCount(), vector.getReader(), builder, millisInUnit == 1? m -> m : m -> millisInUnit * m, parentVectorIsNull, zoneId));
            }
            else {
                // Trino represents LongTimestampType as a long (milliseconds from epoch) + an int (millisecond fraction, in picoseconds)
                vectors.forEach(vector -> copyTimestampNanosTimeZone(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull, zoneId));
            }
            return builder.build();
        }
        else if (type instanceof ArrayType arrayType) {
            Field subfield = Iterators.getOnlyElement(field.getChildren().iterator());
            List<FieldVector> subvectors = vectors
                    .stream()
                    .map(vector -> Iterators.getOnlyElement(vector.getChildrenFromFields().iterator()))
                    .collect(Collectors.toList());
            int subpositions = subvectors.stream().mapToInt(ValueVector::getValueCount).sum();
            Block values = buildBlock(arrayType.getElementType(), subfield, subpositions, subvectors, Optional.empty());
            int newPositions;
            int[] offsets;
            if (parentVectorIsNull.isPresent()) {
                offsets = buildOffsetsUsingParentNullsVector(vectors, parentVectorIsNull.orElseThrow());
                newPositions = offsets.length - 1;
                LOG.debug("QueryData(%s) Array column - parent nulls vector is present=%s, offsets=%s, newPositions=%s",
                        traceStr, Arrays.toString(compactedIsNull.orElseThrow()), Arrays.toString(offsets), newPositions);
            }
            else {
                offsets = buildOffsets(positions, vectors);
                newPositions = positions;
                LOG.debug("QueryData(%s) Array column - parent nulls vector is not present, using self compacted=%s, offsets=%s, newPositions=%s",
                        traceStr, compactedIsNull.map(Arrays::toString).orElse("empty"), Arrays.toString(offsets), newPositions);
            }
            Block block = ArrayBlock.fromElementBlock(newPositions, compactedIsNull, offsets, values);
            LOG.debug("QueryData(%s) Array column - returning block with positions=%s, nulls=%s, offsets=%s, childBlock=%s: %s",
                    traceStr, positions, compactedIsNull.map(Arrays::toString).orElse("empty"), Arrays.toString(offsets), values, block);
            return block;
        }
        else if (type instanceof RowType rowType) { // Map is two separate row blocks, built here
            List<RowType.Field> nestedFields = rowType.getFields();
            int numberOfNestedFields = nestedFields.size();
            ArrayList<ArrayList<FieldVector>> nestedFieldsVectors = new ArrayList<>(numberOfNestedFields);
            for (int i = 0; i < numberOfNestedFields; i++) {
                nestedFieldsVectors.add(new ArrayList<>(vectors.size()));
            }
            vectors.forEach(vector -> {
                List<FieldVector> nestedVectors = vector.getChildrenFromFields();
                for (int i = 0; i < numberOfNestedFields; i++) {
                    FieldVector fieldVector = nestedVectors.get(i);
                    nestedFieldsVectors.get(i).add(fieldVector);
                }
            });
            Optional<boolean[]> rowIsNullForChildren;
            int rowPositionCount;
            if (parentVectorIsNull.isPresent()) {
                LOG.debug("QueryData(%s) Row column - parent null vector is present and of size=%s", traceStr, parentVectorIsNull.orElseThrow().length);
                rowIsNullForChildren = parentVectorIsNull;
                rowPositionCount = compactedIsNull.orElseThrow().length;
            }
            else {
                rowIsNullForChildren = compactedIsNull;
                rowPositionCount = positions;
                LOG.debug("QueryData(%s) Row column - parent null vector is empty, passing self compacted null vector of size=%s, rowPositionCount=%s",
                        traceStr, compactedIsNull.isPresent() ? compactedIsNull.orElseThrow().length : "empty", rowPositionCount);
            }
            Block[] nestedBlocks = new Block[numberOfNestedFields];
            for (int i = 0; i < numberOfNestedFields; i++) {
                ArrayList<FieldVector> nestedFieldVectors = nestedFieldsVectors.get(i);
                Field nestedArrowField = nestedFieldVectors.getFirst().getField();
                Type nestedTrinoType = nestedFields.get(i).getType();
                LOG.debug("QueryData(%s) Row column - calling buildBlock. nestedTrinoType=%s, nestedArrowField=%s, positions=%s, rowIsNullForChildren=%s",
                        traceStr, nestedTrinoType, nestedArrowField, positions, rowIsNullForChildren);
                nestedBlocks[i] = buildBlock(nestedTrinoType, nestedArrowField, positions, nestedFieldVectors, rowIsNullForChildren);
            }
            LOG.debug("QueryData(%s) Row column - constructing from field blocks. positions=%s (original positions=%s), nulls=%s, blocks=%s",
                    traceStr, rowPositionCount, positions, compactedIsNull.map(Arrays::toString).orElse("empty"), Arrays.asList(nestedBlocks));
            return RowBlock.fromNotNullSuppressedFieldBlocks(rowPositionCount, compactedIsNull, nestedBlocks);
        }
        else if (UUID.equals(type)) {
            BlockBuilder builder = type.createBlockBuilder(new PageBuilderStatus(positions * 16).createBlockBuilderStatus(), positions);
            vectors.forEach(vector -> copyUuids(vector.getValueCount(), vector.getReader(), builder));
            return builder.build();
        }
        throw new UnsupportedOperationException(format("QueryData(%s) unsupported %s type: %s", traceStr, type, vectors));
    }

    private short[] byteArrayAsShortArray(byte[] byteArray)
    {
        int shortCount = byteArray.length / Short.BYTES;
        short[] shortArray = new short[shortCount];
        unsafe.copyMemory(byteArray, Unsafe.ARRAY_BYTE_BASE_OFFSET, shortArray, Unsafe.ARRAY_SHORT_BASE_OFFSET, shortCount * Short.BYTES);
        return shortArray;
    }

    private int[] byteArrayAsIntArray(byte[] byteArray)
    {
        int intCount = byteArray.length / Integer.BYTES;
        int[] intArray = new int[intCount];
        unsafe.copyMemory(byteArray, Unsafe.ARRAY_BYTE_BASE_OFFSET, intArray, Unsafe.ARRAY_INT_BASE_OFFSET, intCount * Integer.BYTES);
        return intArray;
    }

    private long[] byteArrayAsLongArray(byte[] byteArray)
    {
        int longCount = byteArray.length / Long.BYTES;
        long[] longArray = new long[longCount];
        unsafe.copyMemory(byteArray, Unsafe.ARRAY_BYTE_BASE_OFFSET, longArray, Unsafe.ARRAY_LONG_BASE_OFFSET, longCount * Long.BYTES);
        return longArray;
    }

    private int[] buildOffsetsUsingParentNullsVector(List<FieldVector> vectors, boolean[] parentNulls)
    {
        int[] offsets = new int[parentNulls.length + 1];
        int entry = 1; // first offset is always 0
        int parentRelativeIndex = 0;
        for (FieldVector vector : vectors) {
            ArrowBuf offsetBuffer = vector.getOffsetBuffer();
            int vectorOffsetsBase = offsets[entry - 1];
            for (int i = 0; i < vector.getValueCount(); ++i) {
                if (!parentNulls[parentRelativeIndex]) {
                    offsets[entry] = vectorOffsetsBase + offsetBuffer.getInt((long) OFFSET_WIDTH * (i + 1));
                }
                else {
                    offsets[entry] = offsets[entry - 1];
                }
                entry++;
                parentRelativeIndex++;
            }
        }
        return Arrays.copyOfRange(offsets, 0, entry);
    }

    private int[] buildOffsets(int positions, List<FieldVector> vectors)
    {
        int expectedOffsetsArrayLength = positions + 1;
        int[] offsets = new int[expectedOffsetsArrayLength];
        int entry = 1; // first offset is always 0
        for (FieldVector vector : vectors) {
            // TODO: consider optimizing this loop
            ArrowBuf offsetBuffer = vector.getOffsetBuffer();
            int vectorOffsetsBase = offsets[entry - 1];
            for (int i = 0; i < vector.getValueCount(); ++i) {
                offsets[entry] = vectorOffsetsBase + offsetBuffer.getInt((long) (i + 1) * OFFSET_WIDTH);
                entry++;
            }
        }
        verify(entry == expectedOffsetsArrayLength, "converted %s offsets, instead of %s", entry, expectedOffsetsArrayLength);
        return offsets;
    }

    static Optional<boolean[]> buildIsNull(int positions, List<FieldVector> vectors)
    {
        if (vectors.stream().allMatch(vector -> vector.getNullCount() == 0)) {
            return Optional.empty();
        }
        boolean[] valueIsNull = new boolean[positions];
        int position = 0;
        for (FieldVector vector : vectors) {
            final int valueCount = vector.getValueCount();
            ArrowBuf validityBuffer = vector.getValidityBuffer();
            int byteIndex;
            for (byteIndex = 0; byteIndex < valueCount / 8; ++byteIndex) {
                int index = validityBuffer.getByte(byteIndex) & 0x00FF; // convert to an unsigned integer (between 0 and 255)
                System.arraycopy(NULL_UNPACKING_TABLE[index], 0, valueIsNull, position, 8);
                position += 8;
            }
            int leftover = valueCount - byteIndex * 8;
            if (leftover > 0) {
                int index = validityBuffer.getByte(byteIndex) & 0x00FF;
                System.arraycopy(NULL_UNPACKING_TABLE[index], 0, valueIsNull, position, leftover);
                position += leftover;
            }
        }
        verify(position == positions, "converted %s NULLs, instead of %s", position, positions);
        return Optional.of(valueIsNull);
    }

    private void copyDataBuffers(Slice dst, List<FieldVector> vectors)
    {
        int offset = 0;
        for (FieldVector vector : vectors) {
            ArrowBuf data = vector.getDataBuffer();
            if (data.readableBytes() == 0) {
                continue; // 0-sized Slices are not supported
            }
            //TODO: don't copy and allocate if unneeded ORION-156020
            Slice dataSlice = Slices.wrappedBuffer(byteBufferToArray(data.nioBuffer()));
            dst.setBytes(offset, dataSlice);
            offset += dataSlice.length();
        }
        verify(offset == dst.length(), "total data size (%s) doesn't match slice size (%s)", offset, dst.length());
    }

    private byte[] byteBufferToArray(ByteBuffer byteBuffer)
    {
        byte[] br = new byte[byteBuffer.remaining()];
        byteBuffer.get(br);
        return br;
    }

    private static void copyTimeNanos(int count, FieldReader reader, BlockBuilder builder, Optional<boolean[]> optionalParentVectorIsNull)
    {
        if (optionalParentVectorIsNull.isEmpty()) {
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (reader.isSet()) {
                    long micros = reader.readLong();
                    long picos = micros * 1_000L;
                    ((LongArrayBlockBuilder) builder).writeLong(picos);
                }
                else {
                    builder.appendNull();
                }
            }
        }
        else {
            boolean[] parentNullsBitmap = optionalParentVectorIsNull.orElseThrow();
            int builderPosition = builder.getPositionCount();
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (!parentNullsBitmap[i + builderPosition]) {
                    if (reader.isSet()) {
                        long micros = reader.readLong();
                        long picos = micros * 1_000L;
                        ((LongArrayBlockBuilder) builder).writeLong(picos);
                    }
                    else {
                        builder.appendNull();
                    }
                }
                else {
                    builder.appendNull();
                }
            }
        }
    }

    private static void copyTimeMicros(int count, FieldReader reader, BlockBuilder builder, Optional<boolean[]> optionalParentVectorIsNull)
    {
        if (optionalParentVectorIsNull.isEmpty()) {
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (reader.isSet()) {
                    long micros = reader.readLong();
                    long picos = micros * 1_000_000L;
                    ((LongArrayBlockBuilder) builder).writeLong(picos);
                }
                else {
                    builder.appendNull();
                }
            }
        }
        else {
            boolean[] parentNullsBitmap = optionalParentVectorIsNull.orElseThrow();
            int builderPosition = builder.getPositionCount();
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (!parentNullsBitmap[i + builderPosition]) {
                    if (reader.isSet()) {
                        long micros = reader.readLong();
                        long picos = micros * 1_000_000L;
                        ((LongArrayBlockBuilder) builder).writeLong(picos);
                    }
                    else {
                        builder.appendNull();
                    }
                }
                else {
                    builder.appendNull();
                }
            }
        }
    }

    private static void copyTimeMillis(int count, TimeMilliVector vector, BlockBuilder builder, Optional<boolean[]> optionalParentVectorIsNull)
    {
        if (optionalParentVectorIsNull.isEmpty()) {
            for (int i = 0; i < count; ++i) {
                if (vector.isSet(i) != 0) {
                    int millis = vector.get(i);
                    long picos = millis * 1_000_000_000L;
                    ((LongArrayBlockBuilder) builder).writeLong(picos);
                }
                else {
                    builder.appendNull();
                }
            }
        }
        else {
            boolean[] parentNullsBitmap = optionalParentVectorIsNull.orElseThrow();
            int builderPosition = builder.getPositionCount();
            for (int i = 0; i < count; ++i) {
                if (!parentNullsBitmap[i + builderPosition]) {
                    if (vector.isSet(i) != 0) {
                        int millis = vector.get(i);
                        long picos = millis * 1_000_000_000L;
                        ((LongArrayBlockBuilder) builder).writeLong(picos);
                    }
                    else {
                        builder.appendNull();
                    }
                }
                else {
                    builder.appendNull();
                }
            }
        }
    }

    private static void copyTimeSeconds(int count, FieldReader reader, BlockBuilder builder, Optional<boolean[]> optionalParentVectorIsNull)
    {
        if (optionalParentVectorIsNull.isEmpty()) {
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (reader.isSet()) {
                    int seconds = reader.readInteger();
                    long picos = seconds * 1_000_000_000_000L;
                    ((LongArrayBlockBuilder) builder).writeLong(picos);
                }
                else {
                    builder.appendNull();
                }
            }
        }
        else {
            boolean[] parentNullsBitmap = optionalParentVectorIsNull.orElseThrow();
            int builderPosition = builder.getPositionCount();
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (!parentNullsBitmap[i + builderPosition]) {
                    if (reader.isSet()) {
                        int seconds = reader.readInteger();
                        long picos = seconds * 1_000_000_000_000L;
                        ((LongArrayBlockBuilder) builder).writeLong(picos);
                    }
                    else {
                        builder.appendNull();
                    }
                }
                else {
                    builder.appendNull();
                }
            }
        }
    }

    private static void copyShortTimestamp(int count, FieldReader reader, BlockBuilder builder, LongFunction<Long> unitConversion, Optional<boolean[]> optionalParentVectorIsNull)
    {
        if (optionalParentVectorIsNull.isEmpty()) {
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (reader.isSet()) {
                    long micros = unitConversion.apply(reader.readLong());
                    ((LongArrayBlockBuilder) builder).writeLong(micros);
                }
                else {
                    builder.appendNull();
                }
            }
        }
        else {
            boolean[] parentNullsBitmap = optionalParentVectorIsNull.orElseThrow();
            int builderPosition = builder.getPositionCount();
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (!parentNullsBitmap[i + builderPosition]) {
                    if (reader.isSet()) {
                        long micros = unitConversion.apply(reader.readLong());
                        ((LongArrayBlockBuilder) builder).writeLong(micros);
                    }
                    else {
                        builder.appendNull();
                    }
                }
                else {
                    builder.appendNull();
                }
            }
        }
    }

    private static void copyShortTimestampWithTimezone(int count, FieldReader reader, BlockBuilder builder, LongFunction<Long> unitConversion, Optional<boolean[]> optionalParentVectorIsNull, String zoneId)
    {
        final TimeZoneKey zoneKey = getTimeZoneKey(zoneId);
        if (optionalParentVectorIsNull.isEmpty()) {
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (reader.isSet()) {
                    long millis = unitConversion.apply(reader.readLong());
                    ((LongArrayBlockBuilder) builder).writeLong(packDateTimeWithZone(millis, zoneKey));
                }
                else {
                    builder.appendNull();
                }
            }
        }
        else {
            boolean[] parentNullsBitmap = optionalParentVectorIsNull.orElseThrow();
            int builderPosition = builder.getPositionCount();
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (!parentNullsBitmap[i + builderPosition]) {
                    if (reader.isSet()) {
                        long millis = unitConversion.apply(reader.readLong());
                        ((LongArrayBlockBuilder) builder).writeLong(packDateTimeWithZone(millis, zoneKey));
                    }
                    else {
                        builder.appendNull();
                    }
                }
                else {
                    builder.appendNull();
                }
            }
        }
    }

    private static void copyTimestampNanos(int count, FieldReader reader, BlockBuilder builder, Optional<boolean[]> optionalParentVectorIsNull)
    {
        if (optionalParentVectorIsNull.isEmpty()) {
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (reader.isSet()) {
                    long nanos = reader.readLong();
                    TypeUtils.Pair<Long, Integer> objects = TypeUtils.convertLongNanoToTwoValues(nanos);
                    ((Fixed12BlockBuilder) builder).writeFixed12(objects.first(), objects.second());
                }
                else {
                    builder.appendNull();
                }
            }
        }
        else {
            boolean[] parentNullsBitmap = optionalParentVectorIsNull.orElseThrow();
            int builderPosition = builder.getPositionCount();
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (!parentNullsBitmap[i + builderPosition]) {
                    if (reader.isSet()) {
                        long nanos = reader.readLong();
                        TypeUtils.Pair<Long, Integer> objects = TypeUtils.convertLongNanoToTwoValues(nanos);
                        ((Fixed12BlockBuilder) builder).writeFixed12(objects.first(), objects.second());
                    }
                    else {
                        builder.appendNull();
                    }
                }
                else {
                    builder.appendNull();
                }
            }
        }
    }

    private static void copyTimestampNanosTimeZone(int count, FieldReader reader, BlockBuilder builder, Optional<boolean[]> optionalParentVectorIsNull, String zoneId)
    {
        final TimeZoneKey zoneKey = getTimeZoneKey(zoneId);
        if (optionalParentVectorIsNull.isEmpty()) {
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (reader.isSet()) {
                    long nanos = reader.readLong();
                    TypeUtils.Pair<Long, Integer> objects = TypeUtils.convertLongNanoToTwoValuesZone(nanos, zoneKey);
                    ((Fixed12BlockBuilder) builder).writeFixed12(objects.first(), objects.second());
                }
                else {
                    builder.appendNull();
                }
            }
        }
        else {
            boolean[] parentNullsBitmap = optionalParentVectorIsNull.orElseThrow();
            int builderPosition = builder.getPositionCount();
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (!parentNullsBitmap[i + builderPosition]) {
                    if (reader.isSet()) {
                        long nanos = reader.readLong();
                        TypeUtils.Pair<Long, Integer> objects = TypeUtils.convertLongNanoToTwoValuesZone(nanos, zoneKey);
                        ((Fixed12BlockBuilder) builder).writeFixed12(objects.first(), objects.second());
                    }
                    else {
                        builder.appendNull();
                    }
                }
                else {
                    builder.appendNull();
                }
            }
        }
    }

    private static void copyChars(int count, FieldReader reader, BlockBuilder builder)
    {
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (reader.isSet()) {
                byte[] bytes = reader.readByteArray();
                Slice slice = trimTrailingSpaces(Slices.wrappedBuffer(bytes));
                ((VariableWidthBlockBuilder) builder).writeEntry(slice);
            }
            else {
                builder.appendNull();
            }
        }
    }

    private static void copyUuids(int count, FieldReader reader, BlockBuilder builder)
    {
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (reader.isSet()) {
                byte[] bytes = reader.readByteArray();
                ByteBuffer bb = ByteBuffer.wrap(bytes, 0, 16).order(ByteOrder.LITTLE_ENDIAN);
                long high = bb.getLong();
                long low = bb.getLong();
                ((Int128ArrayBlockBuilder) builder).writeInt128(high, low);
            }
            else {
                builder.appendNull();
            }
        }
    }

    private static void copyCharsUsingParentNullsBitmap(int count, FieldReader reader, BlockBuilder builder, boolean[] parentNullsBitmap)
    {
        int builderPosition = builder.getPositionCount();
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (!parentNullsBitmap[i + builderPosition]) {
                if (reader.isSet()) {
                    byte[] bytes = reader.readByteArray();
                    Slice slice = trimTrailingSpaces(Slices.wrappedBuffer(bytes));
                    ((VariableWidthBlockBuilder) builder).writeEntry(slice);
                }
                else {
                    builder.appendNull();
                }
            }
            else {
                builder.appendNull();
            }
        }
    }

    private static void copyShortDecimal(int count, FieldReader reader, BlockBuilder builder)
    {
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (reader.isSet()) {
                ((LongArrayBlockBuilder) builder).writeLong(reader.readBigDecimal().unscaledValue().longValueExact());
            }
            else {
                builder.appendNull();
            }
        }
    }

    private static void copyShortDecimalUsingParentNullsBitmap(int count, FieldReader reader, BlockBuilder builder, boolean[] parentNullsBitmap)
    {
        int builderPosition = builder.getPositionCount();
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (!parentNullsBitmap[i + builderPosition]) {
                if (reader.isSet()) {
                    ((LongArrayBlockBuilder) builder).writeLong(reader.readBigDecimal().unscaledValue().longValueExact());
                }
                else {
                    builder.appendNull();
                }
            }
            else {
                builder.appendNull();
            }
        }
    }

    private static void copyLongDecimal(int count, FieldReader reader, BlockBuilder builder, DecimalType decimalType)
    {
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (reader.isSet()) {
                Decimals.writeBigDecimal(decimalType, builder, reader.readBigDecimal());
            }
            else {
                builder.appendNull();
            }
        }
    }

    private static void copyLongDecimalUsingParentNullsBitmap(int count, FieldReader reader, BlockBuilder builder, DecimalType decimalType, boolean[] parentNullsBitmap)
    {
        int builderPosition = builder.getPositionCount();
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (!parentNullsBitmap[i + builderPosition]) {
                if (reader.isSet()) {
                    Decimals.writeBigDecimal(decimalType, builder, reader.readBigDecimal());
                }
                else {
                    builder.appendNull();
                }
            }
            else {
                builder.appendNull();
            }
        }
    }

    private static void copyFloat(int count, FieldReader reader, BlockBuilder builder, boolean[] parentNullsBitmap)
    {
        int builderPosition = builder.getPositionCount();
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (!parentNullsBitmap[i + builderPosition]) {
                if (reader.isSet()) {
                    ((IntArrayBlockBuilder) builder).writeInt(floatToIntBits(reader.readFloat()));
                }
                else {
                    builder.appendNull();
                }
            }
            else {
                builder.appendNull();
            }
        }
    }

    private static void copyDouble(int count, FieldReader reader, BlockBuilder builder, boolean[] parentNullsBitmap)
    {
        int builderPosition = builder.getPositionCount();
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (!parentNullsBitmap[i + builderPosition]) {
                if (reader.isSet()) {
                    ((LongArrayBlockBuilder) builder).writeLong(doubleToLongBits(reader.readDouble()));
                }
                else {
                    builder.appendNull();
                }
            }
            else {
                builder.appendNull();
            }
        }
    }

    private static void copyLong(int count, FieldReader reader, BlockBuilder builder, boolean[] parentNullsBitmap)
    {
        int builderPosition = builder.getPositionCount();
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (!parentNullsBitmap[i + builderPosition]) {
                if (reader.isSet()) {
                    ((LongArrayBlockBuilder) builder).writeLong(reader.readLong());
                }
                else {
                    builder.appendNull();
                }
            }
            else {
                builder.appendNull();
            }
        }
    }

    private static void copyInt(int count, FieldReader reader, BlockBuilder builder, boolean[] parentNullsBitmap)
    {
        int builderPosition = builder.getPositionCount();
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (!parentNullsBitmap[i + builderPosition]) {
                if (reader.isSet()) {
                    ((IntArrayBlockBuilder) builder).writeInt(reader.readInteger());
                }
                else {
                    builder.appendNull();
                }
            }
            else {
                builder.appendNull();
            }
        }
    }

    private static void copySmallInt(int count, FieldReader reader, BlockBuilder builder, boolean[] parentNullsBitmap)
    {
        int builderPosition = builder.getPositionCount();
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (!parentNullsBitmap[i + builderPosition]) {
                if (reader.isSet()) {
                    ((ShortArrayBlockBuilder) builder).writeShort(reader.readShort());
                }
                else {
                    builder.appendNull();
                }
            }
            else {
                builder.appendNull();
            }
        }
    }

    private static void copyTinyInt(int count, FieldReader reader, BlockBuilder builder, boolean[] parentNullsBitmap)
    {
        int builderPosition = builder.getPositionCount();
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (!parentNullsBitmap[i + builderPosition]) {
                if (reader.isSet()) {
                    ((ByteArrayBlockBuilder) builder).writeByte(reader.readByte());
                }
                else {
                    builder.appendNull();
                }
            }
            else {
                builder.appendNull();
            }
        }
    }

    private static void copyBoolean(int count, FieldReader reader, BlockBuilder builder)
    {
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (reader.isSet()) {
                int readBoolean = reader.readBoolean() ? 1 : 0;
                ((ByteArrayBlockBuilder) builder).writeByte((byte) readBoolean);
            }
            else {
                builder.appendNull();
            }
        }
    }

    private static void copyBooleanUsingParentNullsBitmap(int count, FieldReader reader, BlockBuilder builder, boolean[] parentNullsBitmap)
    {
        int builderPosition = builder.getPositionCount();
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (!parentNullsBitmap[i + builderPosition]) {
                if (reader.isSet()) {
                    int readBoolean = reader.readBoolean() ? 1 : 0;
                    ((ByteArrayBlockBuilder) builder).writeByte((byte) readBoolean);
                }
                else {
                    builder.appendNull();
                }
            }
            else {
                builder.appendNull();
            }
        }
    }
}
