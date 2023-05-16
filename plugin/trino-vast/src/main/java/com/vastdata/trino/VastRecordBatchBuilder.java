/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.base.CharMatcher;
import com.vastdata.ValueEntryFunctionFactory;
import com.vastdata.ValueEntryGetter;
import com.vastdata.ValueEntrySetter;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt8Vector;
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
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.OFFSET_WIDTH;

public class VastRecordBatchBuilder
{
    private static final Logger LOG = Logger.get(VastRecordBatchBuilder.class);
    private static final BufferAllocator allocator = new RootAllocator();
    private static final Optional<String> PRINTING_RECEIVED_BLOCK_PREFIX = Optional.of("Printing received");

    private final Schema schema;

    public VastRecordBatchBuilder(Schema schema)
    {
        this.schema = schema;
    }

    public void checkLeaks()
    {
        verify(allocator.getAllocatedMemory() == 0, "%s leaked", allocator);
    }

    private void printBlock(Block block)
    {
        TypeUtils.printBlock(block, PRINTING_RECEIVED_BLOCK_PREFIX);
    }

    public VectorSchemaRoot build(Page page)
    {
        int rows = page.getPositionCount();
        int columns = page.getChannelCount();
        LOG.debug("converting Trino page (%d rows, %d columns) to Arrow (%s)", rows, columns, schema);
        verify(columns == schema.getFields().size());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        IntStream.range(0, columns).forEach(c -> {
            Block block = page.getBlock(c);
            FieldVector vector = root.getVector(c);
            if (LOG.isDebugEnabled()) {
                LOG.debug("copying %s (%d positions, %d bytes) into %s", block, block.getPositionCount(), block.getSizeInBytes(), vector);
            }
            copyData(Optional.empty(), block, vector);
        });
        root.setRowCount(rows);
        return root;
    }

    private void copyData(Optional<Block> optionalParent, Block block, FieldVector vector)
    {
        Field field = vector.getField();
        ArrowType arrowType = field.getType();
        Type trinoType = TypeUtils.convertArrowFieldToTrinoType(field);
        Block parentBlock = optionalParent.orElse(block);
        boolean nestedIsParent = parentBlock.equals(block);
        int positionCount = parentBlock.getPositionCount();
        switch (arrowType.getTypeID()) {
            case Int: {
                ArrowType.Int type = (ArrowType.Int) arrowType;
                if (!type.getIsSigned() && !TypeUtils.isRowId(field)) {
                    throw new UnsupportedOperationException("Unsupported unsigned integer: " + type);
                }
                switch (type.getBitWidth()) {
                    case 8:
                        TinyIntVector tinyIntVector = (TinyIntVector) vector;
                        tinyIntVector.allocateNew(parentBlock.getPositionCount());
                        ValueEntryGetter<Byte> byteGetter = ValueEntryFunctionFactory.newGetter(x -> block.getByte(x, 0), block::isNull, parentBlock::isNull);
                        copyTypeValues(ValueEntryFunctionFactory.newSetter(tinyIntVector::set, tinyIntVector::setNull), nestedIsParent, positionCount, byteGetter);
                        vector.setValueCount(positionCount);
                        return;
                    case 16:
                        SmallIntVector smallIntVector = (SmallIntVector) vector;
                        smallIntVector.allocateNew(parentBlock.getPositionCount());
                        ValueEntryGetter<Short> shortGetter = ValueEntryFunctionFactory.newGetter(x -> block.getShort(x, 0), block::isNull, parentBlock::isNull);
                        copyTypeValues(ValueEntryFunctionFactory.newSetter(smallIntVector::set, smallIntVector::setNull), nestedIsParent, positionCount, shortGetter);
                        vector.setValueCount(positionCount);
                        return;
                    case 32:
                        IntVector intVector = (IntVector) vector;
                        intVector.allocateNew(parentBlock.getPositionCount());
                        ValueEntryGetter<Integer> intGetter = ValueEntryFunctionFactory.newGetter(x -> block.getInt(x, 0), block::isNull, parentBlock::isNull);
                        copyTypeValues(ValueEntryFunctionFactory.newSetter(intVector::set, intVector::setNull), nestedIsParent, positionCount, intGetter);
                        vector.setValueCount(positionCount);
                        return;
                    case 64:
                        ValueEntryGetter<Long> longGetter = ValueEntryFunctionFactory.newGetter(x -> block.getLong(x, 0), block::isNull, parentBlock::isNull);
                        if (type.getIsSigned()) {
                            BigIntVector bigIntVector = (BigIntVector) vector;
                            bigIntVector.allocateNew(parentBlock.getPositionCount());
                            copyTypeValues(ValueEntryFunctionFactory.newSetter(bigIntVector::set, bigIntVector::setNull), nestedIsParent, positionCount, longGetter);
                            vector.setValueCount(positionCount);
                        }
                        else {
                            UInt8Vector uInt8Vector = (UInt8Vector) vector;
                            uInt8Vector.allocateNew(parentBlock.getPositionCount());
                            copyTypeValues(ValueEntryFunctionFactory.newSetter(uInt8Vector::set, uInt8Vector::setNull), nestedIsParent, positionCount, longGetter);
                            vector.setValueCount(positionCount);
                        }
                        return;
                    default:
                        throw new UnsupportedOperationException("Unsupported integer size: " + type);
                }
            }
            case FloatingPoint: {
                ArrowType.FloatingPoint type = (ArrowType.FloatingPoint) arrowType;
                switch (type.getPrecision()) {
                    case SINGLE:
                        Float4Vector float4Vector = (Float4Vector) vector;
                        float4Vector.allocateNew(parentBlock.getPositionCount());
                        ValueEntryGetter<Float> floatGetter = ValueEntryFunctionFactory.newGetter(x -> intBitsToFloat(block.getInt(x, 0)), block::isNull, parentBlock::isNull);
                        copyTypeValues(ValueEntryFunctionFactory.newSetter(float4Vector::set, float4Vector::setNull), nestedIsParent, positionCount, floatGetter);
                        vector.setValueCount(positionCount);
                        return;
                    case DOUBLE:
                        Float8Vector float8Vector = (Float8Vector) vector;
                        float8Vector.allocateNew(parentBlock.getPositionCount());
                        ValueEntryGetter<Double> doubleGetter = ValueEntryFunctionFactory.newGetter(x -> longBitsToDouble(block.getLong(x, 0)), block::isNull, parentBlock::isNull);
                        copyTypeValues(ValueEntryFunctionFactory.newSetter(float8Vector::set, float8Vector::setNull), nestedIsParent, positionCount, doubleGetter);
                        vector.setValueCount(positionCount);
                        return;
                    default:
                        throw new UnsupportedOperationException("Unsupported floating-point precision: " + type);
                }
            }
            case Utf8: {
                VarCharVector varCharVector = (VarCharVector) vector;
                int totalBytes = IntStream.range(0, block.getPositionCount()).map(block::getSliceLength).sum();
                varCharVector.allocateNew(totalBytes, parentBlock.getPositionCount());
                ValueEntrySetter<byte[]> byteArraySetter = ValueEntryFunctionFactory.newSetter(varCharVector::set, varCharVector::setNull);
                ValueEntryGetter<byte[]> byteArrayGetter = ValueEntryFunctionFactory.newGetter(x -> {
                    int length = block.getSliceLength(x);
                    return block.getSlice(x, 0, length).getBytes();
                }, block::isNull, parentBlock::isNull);
                copyTypeValues(byteArraySetter, nestedIsParent, positionCount, byteArrayGetter);
                vector.setValueCount(positionCount);
                return;
            }
            case FixedSizeBinary:
                FixedSizeBinaryVector charVector = (FixedSizeBinaryVector) vector;
                charVector.allocateNew(parentBlock.getPositionCount());
                CharType charType = (CharType) trinoType;
                int typeLength = charType.getLength();
                ValueEntrySetter<byte[]> byteArraySetter = ValueEntryFunctionFactory.newSetter(charVector::set, charVector::setNull);
                ValueEntryGetter<byte[]> rightPaddedSliceByteArrayGetter = ValueEntryFunctionFactory.newGetter(x -> {
                    Slice slice = block.getSlice(x, 0, block.getSliceLength(x));
                    String charsAsPaddedString = TypeUtils.rightPadSpaces(slice.toStringUtf8(), typeLength);
                    verify(CharMatcher.ascii().matchesAllOf(charsAsPaddedString), "CHAR type supports only ASCII charset");
                    return charsAsPaddedString.getBytes(StandardCharsets.UTF_8);
                }, block::isNull, parentBlock::isNull);
                copyTypeValues(byteArraySetter, nestedIsParent, positionCount, rightPaddedSliceByteArrayGetter);
                vector.setValueCount(positionCount);
                return;
            case Binary: {
                VarBinaryVector varBinaryVector = (VarBinaryVector) vector;
                int totalBytes = IntStream.range(0, block.getPositionCount()).map(block::getSliceLength).sum();
                varBinaryVector.allocateNew(totalBytes, parentBlock.getPositionCount());
                ValueEntryGetter<byte[]> byteArrayGetter = ValueEntryFunctionFactory.newGetter(x -> {
                    int length = block.getSliceLength(x);
                    return block.getSlice(x, 0, length).getBytes();
                }, block::isNull, parentBlock::isNull);
                copyTypeValues(ValueEntryFunctionFactory.newSetter(varBinaryVector::set, varBinaryVector::setNull), nestedIsParent, positionCount, byteArrayGetter);
                vector.setValueCount(positionCount);
                return;
            }
            case Bool: {
                BitVector bitVector = (BitVector) vector;
                bitVector.allocateNew(parentBlock.getPositionCount());
                ValueEntryGetter<Byte> byteGetter = ValueEntryFunctionFactory.newGetter(x -> block.getByte(x, 0), block::isNull, parentBlock::isNull);
                copyTypeValues(ValueEntryFunctionFactory.newSetter(bitVector::set, bitVector::setNull), nestedIsParent, positionCount, byteGetter);
                vector.setValueCount(positionCount);
                return;
            }
            case Decimal: {
                DecimalType decimalType = (DecimalType) trinoType;
                DecimalVector decimalVector = (DecimalVector) vector;
                decimalVector.allocateNew(parentBlock.getPositionCount());
                ValueEntrySetter<BigDecimal> bigDecimalSetter = ValueEntryFunctionFactory.newSetter(decimalVector::set, decimalVector::setNull);
                ValueEntryGetter<BigDecimal> bigDecimalGetter = ValueEntryFunctionFactory.newGetter(x -> Decimals.readBigDecimal(decimalType, block, x), block::isNull, parentBlock::isNull);
                copyTypeValues(bigDecimalSetter, nestedIsParent, positionCount, bigDecimalGetter);
                vector.setValueCount(positionCount);
                return;
            }
            case Date: {
                DateDayVector dateDayVector = (DateDayVector) vector;
                dateDayVector.allocateNew(parentBlock.getPositionCount());
                ValueEntrySetter<Integer> intSetter = ValueEntryFunctionFactory.newSetter(dateDayVector::set, dateDayVector::setNull);
                ValueEntryGetter<Integer> intGetter = ValueEntryFunctionFactory.newGetter(x -> block.getInt(x, 0), block::isNull, parentBlock::isNull);
                copyTypeValues(intSetter, nestedIsParent, positionCount, intGetter);
                vector.setValueCount(positionCount);
                return;
            }
            case Timestamp: {
                ArrowType.Timestamp timestampType = (ArrowType.Timestamp) arrowType;
                TimeUnit unit = timestampType.getUnit();
                long microsInUnit = TypeUtils.timeUnitToPicos(unit) / 1_000_000L;
                TimeStampVector timeStampVector = (TimeStampVector) vector;
                timeStampVector.allocateNew(parentBlock.getPositionCount());
                ValueEntrySetter<Long> longSetter = ValueEntryFunctionFactory.newSetter(timeStampVector::set, timeStampVector::setNull);
                ValueEntryGetter<Long> timestampAsLongGetter = ValueEntryFunctionFactory.newGetter(x -> {
                    if (unit == TimeUnit.NANOSECOND) {
                        // See LongTimestampType docs
                        long micros = block.getLong(x, 0);
                        int picos = block.getInt(x, 8);
                        return TypeUtils.convertTwoValuesNanoToLong(micros, picos); // as nanoseconds
                    }
                    else {
                        long valueMicros = block.getLong(x, 0); // as microseconds
                        if (valueMicros % microsInUnit != 0) {
                            throw new IllegalArgumentException(format("%s value %d be a multiple of %d", unit, valueMicros, microsInUnit));
                        }
                        return valueMicros / microsInUnit; // rescale to use the correct time unit (for Arrow)
                    }
                }, block::isNull, parentBlock::isNull);
                copyTypeValues(longSetter, nestedIsParent, positionCount, timestampAsLongGetter);
                return;
            }
            case Time: {
                TimeUnit unit = ((ArrowType.Time) arrowType).getUnit();
                switch (unit) {
                    case SECOND: {
                        TimeSecVector v = (TimeSecVector) vector;
                        v.allocateNew(parentBlock.getPositionCount());
                        ValueEntryGetter<Integer> secondsIntGetter = ValueEntryFunctionFactory.newGetter(x -> {
                            long picos = block.getLong(x, 0);
                            long seconds = picos / 1_000_000_000_000L;
                            return toIntExact(seconds);
                        }, block::isNull, parentBlock::isNull);
                        copyTypeValues(ValueEntryFunctionFactory.newSetter(v::set, v::setNull), nestedIsParent, positionCount, secondsIntGetter);
                        return;
                    }
                    case MILLISECOND: {
                        TimeMilliVector v = (TimeMilliVector) vector;
                        v.allocateNew(parentBlock.getPositionCount());
                        ValueEntryGetter<Integer> msIntGetter = ValueEntryFunctionFactory.newGetter(x -> {
                            long picos = block.getLong(x, 0);
                            long seconds = picos / 1_000_000_000L;
                            return toIntExact(seconds);
                        }, block::isNull, parentBlock::isNull);
                        copyTypeValues(ValueEntryFunctionFactory.newSetter(v::set, v::setNull), nestedIsParent, positionCount, msIntGetter);
                        return;
                    }
                    case MICROSECOND: {
                        TimeMicroVector v = (TimeMicroVector) vector;
                        v.allocateNew(parentBlock.getPositionCount());
                        ValueEntryGetter<Long> microsecondLongGetter = ValueEntryFunctionFactory.newGetter(x -> {
                            long picos = block.getLong(x, 0);
                            return picos / 1_000_000L;
                        }, block::isNull, parentBlock::isNull);
                        copyTypeValues(ValueEntryFunctionFactory.newSetter(v::set, v::setNull), nestedIsParent, positionCount, microsecondLongGetter);
                        return;
                    }
                    case NANOSECOND: {
                        TimeNanoVector v = (TimeNanoVector) vector;
                        v.allocateNew(parentBlock.getPositionCount());
                        ValueEntryGetter<Long> microsecondLongGetter = ValueEntryFunctionFactory.newGetter(x -> {
                            long picos = block.getLong(x, 0);
                            return picos / 1_000L;
                        }, block::isNull, parentBlock::isNull);
                        copyTypeValues(ValueEntryFunctionFactory.newSetter(v::set, v::setNull), nestedIsParent, positionCount, microsecondLongGetter);
                        return;
                    }
                    default:
                        throw new UnsupportedOperationException("Unexpected precision time unit: " + unit);
                }
            }
            case Struct: {
                StructVector sv = (StructVector) vector;
                copyStruct(parentBlock, block, sv);
                return;
            }
            case Map: {
                copyMap(trinoType, parentBlock, block, (MapVector) vector);
                return;
            }
            case List: {
                copyList(block, (ListVector) vector);
                return;
            }
            default:
                throw new UnsupportedOperationException("Unsupported Arrow type: " + arrowType);
        }
    }

    private void copyMap(Type trinoType, Block parentBlock, Block nestedBlock, MapVector vector)
    {
        if (nestedBlock instanceof MapBlock) {
            MapBlock mapBlock = (MapBlock) nestedBlock;
            // based on the arrow map vector structure: map(struct(key, value)))
            List<Block> children = mapBlock.getChildren();
            Block keyBlock = children.get(0);
            Block valueBlock = children.get(1);

            StructVector structChildVector = (StructVector) vector.getChildrenFromFields().get(0);
            copyData(Optional.of(keyBlock), keyBlock, structChildVector.getChildrenFromFields().get(0));
            copyData(Optional.of(valueBlock), valueBlock, structChildVector.getChildrenFromFields().get(1));

            int structPositionCount = keyBlock.getPositionCount();
            structChildVector.setValueCount(structPositionCount);
            int validityBufferSize = BitVectorHelper.getValidityBufferSize(structPositionCount);
            try (ArrowBuf validityBuffer = allocator.buffer(validityBufferSize)) {
                validityBuffer.setOne(0, validityBufferSize);
                ArrowFieldNode node = new ArrowFieldNode(structPositionCount, 0);
                structChildVector.loadFieldBuffers(node, List.of(validityBuffer));
            }

            ColumnarMap columnarMap = ColumnarMap.toColumnarMap(nestedBlock);
            int nestedPosition = 0;
            boolean nestedIsParent = parentBlock.equals(nestedBlock);
            int parentPositionCount = parentBlock.getPositionCount();
            try (
                    ArrowBuf validity = allocator.buffer(BitVectorHelper.getValidityBufferSize(parentPositionCount));
                    ArrowBuf offsets = allocator.buffer((long) (parentPositionCount + 1) * OFFSET_WIDTH)) {
                int nullCount = 0;
                int offset = 0;
                offsets.setInt(0, offset);
                for (int i = 0; i < parentPositionCount; ++i) {
                    int offsetIndex = (i + 1) * OFFSET_WIDTH;
                    if (parentBlock.isNull(i)) {
                        BitVectorHelper.unsetBit(validity, i);
                        offsets.setInt(offsetIndex, offset);
                        if (nestedIsParent) {
                            nestedPosition++;
                        }
                    }
                    else {
                        if (columnarMap.isNull(nestedPosition)) {
                            nullCount += 1;
                            BitVectorHelper.unsetBit(validity, i);
                        }
                        else {
                            BitVectorHelper.setBit(validity, i);
                            offset = columnarMap.getOffset(nestedPosition + 1);
                        }
                        offsets.setInt(offsetIndex, offset);
                        nestedPosition++;
                    }
                }
                ArrowFieldNode node = new ArrowFieldNode(parentPositionCount, nullCount);
                vector.loadFieldBuffers(node, List.of(validity, offsets));
                vector.setValueCount(parentPositionCount);
            }
        }
        else if (nestedBlock instanceof RunLengthEncodedBlock) {
            BlockBuilder blockBuilder = trinoType.createBlockBuilder(null, parentBlock.getPositionCount());
            MapBlock mapValueBlock = (MapBlock) ((RunLengthEncodedBlock) nestedBlock).getValue();
            for (int i = 0; i < parentBlock.getPositionCount(); i++) {
                trinoType.appendTo(mapValueBlock, 0, blockBuilder);
            }
            copyMap(trinoType, parentBlock, blockBuilder.build(), vector);
        }
        else {
            throw new UnsupportedOperationException(format("Unexpected block for map type: %s", nestedBlock));
        }
    }

    private void copyStruct(Block parentBlock, Block nestedBlock, StructVector sv)
    {
        boolean nestedIsParent = parentBlock.equals(nestedBlock);
        List<FieldVector> subVectors = sv.getChildrenFromFields();
        ColumnarRow columnarRow = ColumnarRow.toColumnarRow(nestedBlock);
        int fieldCount = columnarRow.getFieldCount();
        int subVectorsSize = subVectors.size();
        verify(subVectorsSize == fieldCount, format("Nested types vectors count do not match: %s/%s", subVectorsSize, fieldCount));
        for (int i = 0; i < fieldCount; i++) {
            FieldVector subVector = subVectors.get(i);
            Block rowField = columnarRow.getField(i);
            copyData(Optional.of(parentBlock), rowField, subVector);
        }
        int positionCount = parentBlock.getPositionCount();
        try (ArrowBuf validityBuffer = allocator.buffer(BitVectorHelper.getValidityBufferSize(positionCount))) {
            int nulls = 0;
            int nestedPosition = 0;
            for (int i = 0; i < positionCount; i++) {
                if (parentBlock.isNull(i)) {
                    BitVectorHelper.unsetBit(validityBuffer, i);
                    nulls++;
                    if (nestedIsParent) {
                        nestedPosition++;
                    }
                }
                else {
                    if (nestedBlock.isNull(nestedPosition)) {
                        BitVectorHelper.unsetBit(validityBuffer, i);
                        nulls++;
                    }
                    else {
                        BitVectorHelper.setBit(validityBuffer, i);
                    }
                    nestedPosition++;
                }
            }
            ArrowFieldNode node = new ArrowFieldNode(positionCount, nulls);
            sv.loadFieldBuffers(node, List.of(validityBuffer));
            sv.setValueCount(positionCount);
        }
    }

    private void copyList(Block block, ListVector listVector)
    {
        FieldVector childVector = listVector.getChildrenFromFields().get(0);
        ColumnarArray columnarArray = ColumnarArray.toColumnarArray(block);
        copyData(Optional.empty(), columnarArray.getElementsBlock(), childVector);

        int positionCount = columnarArray.getPositionCount();
        try (
                ArrowBuf validity = allocator.buffer(BitVectorHelper.getValidityBufferSize(positionCount));
                ArrowBuf offsets = allocator.buffer((long) (positionCount + 1) * OFFSET_WIDTH)) {
            int nullCount = 0;
            int offset = 0;
            offsets.setInt(0, offset);
            for (int i = 0; i < positionCount; ++i) {
                if (columnarArray.isNull(i)) {
                    nullCount += 1;
                    BitVectorHelper.unsetBit(validity, i);
                }
                else {
                    BitVectorHelper.setBit(validity, i);
                }
                offset += columnarArray.getLength(i);
                offsets.setInt((long) (i + 1) * OFFSET_WIDTH, offset);
            }
            ArrowFieldNode node = new ArrowFieldNode(positionCount, nullCount);
            listVector.loadFieldBuffers(node, List.of(validity, offsets));
            listVector.setValueCount(positionCount);
        }
    }

    <T> void copyTypeValues(ValueEntrySetter<T> typeSetter, boolean nestedIsParent, int positionCount, ValueEntryGetter<T> typeGetter)
    {
        int nestedPosition = 0;
        for (int i = 0; i < positionCount; ++i) {
            if (typeGetter.isParentNull(i)) {
                typeSetter.setNull(i);
                if (nestedIsParent) {
                    nestedPosition++;
                }
            }
            else {
                if (typeGetter.isNull(nestedPosition)) {
                    typeSetter.setNull(i);
                }
                else {
                    typeSetter.set(i, typeGetter.get(nestedPosition));
                }
                nestedPosition++;
            }
        }
    }
}
