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
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.IntArrayBlockBuilder;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.trimTrailingSpaces;
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
import static io.trino.spi.type.TinyintType.TINYINT;
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
    private final Optional<String> printBlockPrefix;

    public VastPageBuilder(String traceStr, Schema schema)
    {
        this.traceStr = traceStr;
        printBlockPrefix = Optional.of(format("QueryData(%s) Printing resulted", traceStr));
        this.schema = schema;
        this.batches = new ArrayList<>(DEFAULT_BATCHES_CAPACITY);
    }

    @Override
    public VastPageBuilder add(VectorSchemaRoot root)
    {
        batches.add(root);
        return this;
    }

    private void printBlock(Block block)
    {
        TypeUtils.printBlock(block, printBlockPrefix);
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
        if (type instanceof TimeType) {
            TimeType timeType = (TimeType) type;
            return timeType.getPrecision() == precision;
        }
        else {
            return false;
        }
    }

    private boolean isTimestampType(Type type, int precision)
    {
        if (type instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) type;
            return timestampType.getPrecision() == precision;
        }
        else {
            return false;
        }
    }

    private Optional<boolean[]> compactIsNull(Optional<boolean[]> vectorIsNull, Optional<boolean[]> parentVectorIsNull)
    {
        if (parentVectorIsNull.isEmpty()) {
            return vectorIsNull;
        }
        else {
            boolean[] parentNulls = parentVectorIsNull.get();
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
                boolean[] childNulls = vectorIsNull.get();
                boolean[] newChildNulls = new boolean[childNulls.length];
                int newSize = 0;
                for (int i = 0; i < childNulls.length; i++) {
                    if (!parentNulls[i]) {
                        newChildNulls[newSize++] = childNulls[i];
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
                int[] offsets = buildOffsetsUsingParentNullsVector(vectors, parentVectorIsNull.get());
                verify(offsets[offsets.length - 1] == totalBytes, "QueryData(%s) last offset is %s, instead of %s", traceStr, offsets[offsets.length - 1], totalBytes);
                LOG.info("QueryData(%s) VARTRACE returning VariableWidthBlock block for type %s", traceStr, type);
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
                BlockBuilder builder = new LongArrayBlockBuilder(null, compactedIsNull.get().length);
                vectors.forEach(vector -> copyLong(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.get()));
                return builder.build();
            }
            else {
                // NOTE: TIMESTAMP(6) has the same representation in Trino & Arrow
                long[] values = new long[positions];
                copyDataBuffers(Slices.wrappedLongArray(values), vectors);
                return new LongArrayBlock(positions, compactedIsNull, values);
            }
        }
        else if (DOUBLE.equals(type)) {
            if (parentVectorIsNull.isPresent()) {
                BlockBuilder builder = new LongArrayBlockBuilder(null, compactedIsNull.get().length);
                vectors.forEach(vector -> copyDouble(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.get()));
                return builder.build();
            }
            else {
                long[] values = new long[positions];
                copyDataBuffers(Slices.wrappedLongArray(values), vectors);
                return new LongArrayBlock(positions, compactedIsNull, values);
            }
        }
        else if (type instanceof CharType) {
            if (parentVectorIsNull.isPresent()) {
                BlockBuilder builder = type.createBlockBuilder(null, compactedIsNull.get().length);
                vectors.forEach(vector -> copyCharsUsingParentNullsBitmap(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.get()));
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
                BlockBuilder builder = TINYINT.createFixedSizeBlockBuilder(compactedIsNull.get().length);
                vectors.forEach(vector -> copyTinyInt(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.get()));
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
                BlockBuilder builder = SMALLINT.createFixedSizeBlockBuilder(compactedIsNull.get().length);
                vectors.forEach(vector -> copySmallInt(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.get()));
                return builder.build();
            }
            else {
                short[] values = new short[positions];
                copyDataBuffers(Slices.wrappedShortArray(values), vectors);
                return new ShortArrayBlock(positions, compactedIsNull, values);
            }
        }
        else if (INTEGER.equals(type) || DATE.equals(type)) {
            if (parentVectorIsNull.isPresent()) {
                BlockBuilder builder = new IntArrayBlockBuilder(null, compactedIsNull.get().length);
                vectors.forEach(vector -> copyInt(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.get()));
                return builder.build();
            }
            else {
                int[] values = new int[positions];
                copyDataBuffers(Slices.wrappedIntArray(values), vectors);
                return new IntArrayBlock(positions, compactedIsNull, values);
            }
        }
        else if (REAL.equals(type)) {
            if (parentVectorIsNull.isPresent()) {
                BlockBuilder builder = new IntArrayBlockBuilder(null, compactedIsNull.get().length);
                vectors.forEach(vector -> copyFloat(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.get()));
                return builder.build();
            }
            else {
                int[] values = new int[positions];
                copyDataBuffers(Slices.wrappedIntArray(values), vectors);
                return new IntArrayBlock(positions, compactedIsNull, values);
            }
        }
        else if (BOOLEAN.equals(type)) {
            BlockBuilder builder;
            if (parentVectorIsNull.isPresent()) {
                builder = BOOLEAN.createFixedSizeBlockBuilder(compactedIsNull.get().length);
                vectors.forEach(vector -> copyBooleanUsingParentNullsBitmap(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.get()));
            }
            else {
                builder = BOOLEAN.createFixedSizeBlockBuilder(positions);
                vectors.forEach(vector -> copyBoolean(vector.getValueCount(), vector.getReader(), builder));
            }
            return builder.build();
        }
        else if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            BlockBuilder builder = decimalType.createFixedSizeBlockBuilder(positions);
            if (parentVectorIsNull.isPresent()) {
                if (decimalType.isShort()) {
                    // convert 128-bit decimal into 64-bit block
                    vectors.forEach(vector -> copyShortDecimalUsingParentNullsBitmap(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull.get()));
                }
                else {
                    vectors.forEach(vector -> copyLongDecimalUsingParentNullsBitmap(vector.getValueCount(), vector.getReader(), builder, decimalType, parentVectorIsNull.get()));
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
        else if (type instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) type;
            BlockBuilder builder = timestampType.createFixedSizeBlockBuilder(positions);
            TimeUnit unit = ((ArrowType.Timestamp) arrowType).getUnit();
            if (timestampType.isShort()) {
                // Trino represents ShortTimestampType as a long (microseconds from epoch)
                long microsInUnit = TypeUtils.timeUnitToPicos(unit) / 1_000_000L;
                verify(microsInUnit > 0, "%s is not supported for %s", unit, timestampType);
                vectors.forEach(vector -> copyShortTimestamp(vector.getValueCount(), vector.getReader(), builder, microsInUnit, parentVectorIsNull));
            }
            else {
                // Trino represents LongTimestampType as a long (microseconds from epoch) + an int (microsecond fraction, in picoseconds)
                vectors.forEach(vector -> copyTimestampNanos(vector.getValueCount(), vector.getReader(), builder, parentVectorIsNull));
            }
            return builder.build();
        }
        else if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
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
                offsets = buildOffsetsUsingParentNullsVector(vectors, parentVectorIsNull.get());
                newPositions = offsets.length - 1;
                LOG.debug("QueryData(%s) Array column - parent nulls vector is present=%s, offsets=%s, newPositions=%s",
                        traceStr, Arrays.toString(compactedIsNull.get()), Arrays.toString(offsets), newPositions);
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
        else if (type instanceof RowType) { // Map is two separate row blocks, built here
            RowType rowType = (RowType) type;
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
                LOG.info("QueryData(%s) Row column - parent null vector is present and of size=%s", traceStr, parentVectorIsNull.get().length);
                rowIsNullForChildren = parentVectorIsNull;
                rowPositionCount = compactedIsNull.get().length;
            }
            else {
                LOG.info("QueryData(%s) Row column - parent null vector is empty, passing self compacted null vector of size=%s", traceStr, compactedIsNull.isPresent() ? compactedIsNull.get().length : "empty");
                rowIsNullForChildren = compactedIsNull;
                rowPositionCount = positions;
            }
            Block[] nestedBlocks = new Block[numberOfNestedFields];
            for (int i = 0; i < numberOfNestedFields; i++) {
                ArrayList<FieldVector> nestedFieldVectors = nestedFieldsVectors.get(i);
                Field nestedArrowField = nestedFieldVectors.get(0).getField();
                Type nestedTrinoType = nestedFields.get(i).getType();
                nestedBlocks[i] = buildBlock(nestedTrinoType, nestedArrowField, positions, nestedFieldVectors, rowIsNullForChildren);
            }
            LOG.info("QueryData(%s) Row column - constructing from field blocks. positions=%s (original positions=%s), nulls=%s, blocks=%s",
                    traceStr, rowPositionCount, positions, compactedIsNull.map(Arrays::toString).orElse("empty"), Arrays.asList(nestedBlocks));
            return RowBlock.fromFieldBlocks(rowPositionCount, compactedIsNull, nestedBlocks);
        }
        throw new UnsupportedOperationException(format("QueryData(%s) unsupported %s type: %s", traceStr, type, vectors));
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
                    entry += 1;
                }
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
            Slice dataSlice = Slices.wrappedBuffer(data.nioBuffer());
            dst.setBytes(offset, dataSlice);
            offset += dataSlice.length();
        }
        verify(offset == dst.length(), "total data size (%s) doesn't match slice size (%s)", offset, dst.length());
    }

    private static void copyTimeNanos(int count, FieldReader reader, BlockBuilder builder, Optional<boolean[]> optionalParentVectorIsNull)
    {
        if (optionalParentVectorIsNull.isEmpty()) {
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (reader.isSet()) {
                    long micros = reader.readLong();
                    long picos = micros * 1_000L;
                    builder.writeLong(picos);
                    builder.closeEntry();
                }
                else {
                    builder.appendNull();
                }
            }
        }
        else {
            boolean[] parentNullsBitmap = optionalParentVectorIsNull.get();
            int builderPosition = builder.getPositionCount();
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (!parentNullsBitmap[i + builderPosition]) {
                    if (reader.isSet()) {
                        long micros = reader.readLong();
                        long picos = micros * 1_000L;
                        builder.writeLong(picos);
                        builder.closeEntry();
                    }
                    else {
                        builder.appendNull();
                    }
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
                    builder.writeLong(picos);
                    builder.closeEntry();
                }
                else {
                    builder.appendNull();
                }
            }
        }
        else {
            boolean[] parentNullsBitmap = optionalParentVectorIsNull.get();
            int builderPosition = builder.getPositionCount();
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (!parentNullsBitmap[i + builderPosition]) {
                    if (reader.isSet()) {
                        long micros = reader.readLong();
                        long picos = micros * 1_000_000L;
                        builder.writeLong(picos);
                        builder.closeEntry();
                    }
                    else {
                        builder.appendNull();
                    }
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
                    builder.writeLong(picos);
                    builder.closeEntry();
                }
                else {
                    builder.appendNull();
                }
            }
        }
        else {
            boolean[] parentNullsBitmap = optionalParentVectorIsNull.get();
            int builderPosition = builder.getPositionCount();
            for (int i = 0; i < count; ++i) {
                if (!parentNullsBitmap[i + builderPosition]) {
                    if (vector.isSet(i) != 0) {
                        int millis = vector.get(i);
                        long picos = millis * 1_000_000_000L;
                        builder.writeLong(picos);
                        builder.closeEntry();
                    }
                    else {
                        builder.appendNull();
                    }
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
                    builder.writeLong(picos);
                    builder.closeEntry();
                }
                else {
                    builder.appendNull();
                }
            }
        }
        else {
            boolean[] parentNullsBitmap = optionalParentVectorIsNull.get();
            int builderPosition = builder.getPositionCount();
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (!parentNullsBitmap[i + builderPosition]) {
                    if (reader.isSet()) {
                        int seconds = reader.readInteger();
                        long picos = seconds * 1_000_000_000_000L;
                        builder.writeLong(picos);
                        builder.closeEntry();
                    }
                    else {
                        builder.appendNull();
                    }
                }
            }
        }
    }

    private static void copyShortTimestamp(int count, FieldReader reader, BlockBuilder builder, long microsInUnit, Optional<boolean[]> optionalParentVectorIsNull)
    {
        if (optionalParentVectorIsNull.isEmpty()) {
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (reader.isSet()) {
                    long micros = reader.readLong() * microsInUnit;
                    builder.writeLong(micros);
                    builder.closeEntry();
                }
                else {
                    builder.appendNull();
                }
            }
        }
        else {
            boolean[] parentNullsBitmap = optionalParentVectorIsNull.get();
            int builderPosition = builder.getPositionCount();
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (!parentNullsBitmap[i + builderPosition]) {
                    if (reader.isSet()) {
                        long micros = reader.readLong() * microsInUnit;
                        builder.writeLong(micros);
                        builder.closeEntry();
                    }
                    else {
                        builder.appendNull();
                    }
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
                    Object[] objects = TypeUtils.convertLongNanoToTwoValues(nanos);
                    builder.writeLong((long) objects[0]);
                    builder.writeInt((int) objects[1]);
                    builder.closeEntry();
                }
                else {
                    builder.appendNull();
                }
            }
        }
        else {
            boolean[] parentNullsBitmap = optionalParentVectorIsNull.get();
            int builderPosition = builder.getPositionCount();
            for (int i = 0; i < count; ++i) {
                reader.setPosition(i);
                if (!parentNullsBitmap[i + builderPosition]) {
                    if (reader.isSet()) {
                        long nanos = reader.readLong();
                        Object[] objects = TypeUtils.convertLongNanoToTwoValues(nanos);
                        builder.writeLong((long) objects[0]);
                        builder.writeInt((int) objects[1]);
                        builder.closeEntry();
                    }
                    else {
                        builder.appendNull();
                    }
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
                builder.writeBytes(slice, 0, slice.length());
                builder.closeEntry();
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
                    builder.writeBytes(slice, 0, slice.length());
                    builder.closeEntry();
                }
                else {
                    builder.appendNull();
                }
            }
        }
    }

    private static void copyShortDecimal(int count, FieldReader reader, BlockBuilder builder)
    {
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (reader.isSet()) {
                builder.writeLong(reader.readBigDecimal().unscaledValue().longValueExact());
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
                    builder.writeLong(reader.readBigDecimal().unscaledValue().longValueExact());
                }
                else {
                    builder.appendNull();
                }
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
        }
    }

    private static void copyFloat(int count, FieldReader reader, BlockBuilder builder, boolean[] parentNullsBitmap)
    {
        int builderPosition = builder.getPositionCount();
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (!parentNullsBitmap[i + builderPosition]) {
                if (reader.isSet()) {
                    builder.writeInt(floatToIntBits(reader.readFloat())).closeEntry();
                }
                else {
                    builder.appendNull();
                }
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
                    builder.writeLong(doubleToLongBits(reader.readDouble())).closeEntry();
                }
                else {
                    builder.appendNull();
                }
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
                    builder.writeLong(reader.readLong()).closeEntry();
                }
                else {
                    builder.appendNull();
                }
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
                    builder.writeInt(reader.readInteger()).closeEntry();
                }
                else {
                    builder.appendNull();
                }
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
                    builder.writeShort(reader.readShort()).closeEntry();
                }
                else {
                    builder.appendNull();
                }
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
                    builder.writeByte(reader.readByte()).closeEntry();
                }
                else {
                    builder.appendNull();
                }
            }
        }
    }

    private static void copyBoolean(int count, FieldReader reader, BlockBuilder builder)
    {
        for (int i = 0; i < count; ++i) {
            reader.setPosition(i);
            if (reader.isSet()) {
                builder.writeByte(reader.readBoolean() ? 1 : 0).closeEntry();
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
                    builder.writeByte(reader.readBoolean() ? 1 : 0).closeEntry();
                }
                else {
                    builder.appendNull();
                }
            }
        }
    }
}
