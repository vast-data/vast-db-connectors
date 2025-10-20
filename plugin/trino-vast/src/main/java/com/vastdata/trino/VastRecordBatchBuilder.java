/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.base.CharMatcher;
import com.vastdata.ValueEntryFunctionFactory;
import com.vastdata.ValueEntryGetter;
import com.vastdata.ValueEntrySetter;
import com.vastdata.trino.block.BlockApiFactory;
import com.vastdata.trino.block.ByteBlockApi;
import com.vastdata.trino.block.Fixed12BlockApi;
import com.vastdata.trino.block.IntBlockApi;
import com.vastdata.trino.block.LongBlockApi;
import com.vastdata.trino.block.ShortBlockApi;
import com.vastdata.trino.block.SliceBlock;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;
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

    private final BufferAllocator allocator = new RootAllocator();
    private final Schema schema;

    public VastRecordBatchBuilder(Schema schema)
    {
        this.schema = schema;
    }

    public void checkLeaks()
    {
        verify(allocator.getAllocatedMemory() == 0, "%s leaked", allocator);
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
                LOG.debug("copying %s (%d positions, %d bytes) into %s", block, block.getPositionCount(), block.getSizeInBytes(), vector.getClass());
            }
            copyData(Optional.empty(), block, vector);
        });
        root.setRowCount(rows);
        return root;
    }

    private void copyData(Optional<Block> optionalParent, Block block, FieldVector vector)
    {
        if (block instanceof DictionaryBlock dictionaryBlock) {
            LOG.info("copyData Got DictionaryBlock, child block: %s", dictionaryBlock.getDictionary());
        }
        Field field = vector.getField();
        ArrowType arrowType = field.getType();
        Type trinoType = TypeUtils.convertArrowFieldToTrinoType(field);
        Block parentBlock = optionalParent.orElse(block);
        boolean nestedIsParent = true;
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
                        ByteBlockApi byteApi = BlockApiFactory.getByteApiInstance(block);
                        ValueEntryGetter<Byte> byteGetter = ValueEntryFunctionFactory.newGetter(byteApi::getByte, block::isNull, parentBlock::isNull);
                        copyTypeValues(ValueEntryFunctionFactory.newSetter(tinyIntVector::set, tinyIntVector::setNull), nestedIsParent, positionCount, byteGetter);
                        vector.setValueCount(positionCount);
                        return;
                    case 16:
                        SmallIntVector smallIntVector = (SmallIntVector) vector;
                        smallIntVector.allocateNew(parentBlock.getPositionCount());
                        ShortBlockApi shortApi = BlockApiFactory.getShortApiInstance(block);
                        ValueEntryGetter<Short> shortGetter = ValueEntryFunctionFactory.newGetter(shortApi::getShort, block::isNull, parentBlock::isNull);
                        copyTypeValues(ValueEntryFunctionFactory.newSetter(smallIntVector::set, smallIntVector::setNull), nestedIsParent, positionCount, shortGetter);
                        vector.setValueCount(positionCount);
                        return;
                    case 32:
                        IntVector baseIntVector = (IntVector) vector;
                        IntBlockApi intApi = BlockApiFactory.getIntApiInstance(block);
                        copyFixedWidthVector(block, baseIntVector, parentBlock, nestedIsParent, positionCount, baseIntVector::set, intApi::getInt);
                        return;
                    case 64:
                        LongBlockApi longApi = BlockApiFactory.getLongApiInstance(block);
                        BaseFixedWidthVector baseFixedWidthVector;
                        BiConsumer<Integer, Long> indexedSetter;
                        if (type.getIsSigned()) {
                            BigIntVector bigIntVector = (BigIntVector) vector;
                            indexedSetter = bigIntVector::set;
                            baseFixedWidthVector = bigIntVector;
                        }
                        else {
                            UInt8Vector uInt8Vector = (UInt8Vector) vector;
                            indexedSetter = uInt8Vector::set;
                            baseFixedWidthVector = uInt8Vector;
                        }
                        copyFixedWidthVector(block, baseFixedWidthVector, parentBlock, nestedIsParent, positionCount,
                                indexedSetter, longApi::getLong);
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
                        IntBlockApi intApi = BlockApiFactory.getIntApiInstance(block);
                        copyFixedWidthVector(block, float4Vector, parentBlock, nestedIsParent, positionCount,
                                float4Vector::set, x1 -> intBitsToFloat(intApi.getInt(x1)));
                        return;
                    case DOUBLE:
                        Float8Vector float8Vector = (Float8Vector) vector;
                        LongBlockApi longApi = BlockApiFactory.getLongApiInstance(block);
                        copyFixedWidthVector(block, float8Vector, parentBlock, nestedIsParent, positionCount, float8Vector::set, x -> longBitsToDouble(longApi.getLong(x)));
                        return;
                    default:
                        throw new UnsupportedOperationException("Unsupported floating-point precision: " + type);
                }
            }
            case Utf8:
            case Binary: {
                SliceBlock sliceApi = BlockApiFactory.getSliceApiInstance(block);
                BaseVariableWidthVector varWidthVector = (BaseVariableWidthVector) vector;
                int totalBytes = IntStream.range(0, block.getPositionCount()).map(sliceApi::getSliceLength).sum();
                copyVarWidthVector(block, varWidthVector, totalBytes, parentBlock, nestedIsParent, positionCount, varWidthVector::set, x -> sliceApi.getSlice(x).getBytes());
                return;
            }
            case FixedSizeBinary: {
                SliceBlock sliceApi = BlockApiFactory.getSliceApiInstance(block);
                FixedSizeBinaryVector charVector = (FixedSizeBinaryVector) vector;
                IntFunction<byte[]> indexedGetter = getIndexFixedSizeByteArrayGetter((CharType) trinoType, sliceApi);
                copyFixedWidthVector(block, charVector, parentBlock, nestedIsParent, positionCount, charVector::set, indexedGetter);
                return;
            }
            case Bool: {
                ByteBlockApi byteApi = BlockApiFactory.getByteApiInstance(block);
                BitVector bitVector = (BitVector) vector;
                bitVector.allocateNew(parentBlock.getPositionCount());
                ValueEntryGetter<Byte> byteGetter = ValueEntryFunctionFactory.newGetter(byteApi::getByte, block::isNull, parentBlock::isNull);
                copyTypeValues(ValueEntryFunctionFactory.newSetter(bitVector::set, bitVector::setNull), nestedIsParent, positionCount, byteGetter);
                vector.setValueCount(positionCount);
                return;
            }
            case Decimal: {
                DecimalType decimalType = (DecimalType) trinoType;
                DecimalVector decimalVector = (DecimalVector) vector;
                copyFixedWidthVector(block, decimalVector, parentBlock, nestedIsParent, positionCount,
                        decimalVector::set, x -> Decimals.readBigDecimal(decimalType, block, x));
                return;
            }
            case Date: {
                DateDayVector baseIntVector = (DateDayVector) vector;
                IntBlockApi intApi = BlockApiFactory.getIntApiInstance(block);
                copyFixedWidthVector(block, baseIntVector, parentBlock, nestedIsParent, positionCount,
                        baseIntVector::set, intApi::getInt);
                return;
            }
            case Timestamp: {
                ArrowType.Timestamp timestampType = (ArrowType.Timestamp) arrowType;
                TimeUnit unit = timestampType.getUnit();
                long microsInUnit = TypeUtils.timeUnitToPicos(unit) / 1_000_000L;
                TimeStampVector timeStampVector = (TimeStampVector) vector;
                IntFunction<Long> longSupplier;
                if (unit == TimeUnit.NANOSECOND) {
                    Fixed12BlockApi fixed12Api = BlockApiFactory.getFixed12ApiInstance(block);
                    longSupplier = x -> {
                        // See LongTimestampType docs
                        long micros = fixed12Api.getFixed12First(x);
                        int picos = fixed12Api.getFixed12Second(x);
                        return TypeUtils.convertTwoValuesNanoToLong(micros, picos);
                    };
                }
                else {
                    LongBlockApi longApi = BlockApiFactory.getLongApiInstance(block);
                    longSupplier = x -> {
                        long valueMicros = longApi.getLong(x); // as microseconds
                        if (valueMicros % microsInUnit != 0) {
                            throw new IllegalArgumentException(format("%s value %d be a multiple of %d", unit, valueMicros, microsInUnit));
                        }
                        return valueMicros / microsInUnit; // rescale to use the correct time unit (for Arrow)
                    };
                }
                copyFixedWidthVector(block, timeStampVector, parentBlock, nestedIsParent, positionCount,
                        timeStampVector::set, longSupplier);
                return;
            }
            case Time: {
                TimeUnit unit = ((ArrowType.Time) arrowType).getUnit();
                LongBlockApi longApi = BlockApiFactory.getLongApiInstance(block);
                BaseFixedWidthVector baseFixedWidthVector = (BaseFixedWidthVector) vector;
                switch (unit) {
                    case SECOND: {
                        TimeSecVector v = (TimeSecVector) vector;
                        IntFunction<Integer> indexedGetter = x -> {
                            long picos = longApi.getLong(x);
                            long seconds = picos / 1_000_000_000_000L;
                            return toIntExact(seconds);
                        };
                        copyFixedWidthVector(block, baseFixedWidthVector, parentBlock, nestedIsParent, positionCount, v::set, indexedGetter);
                        return;
                    }
                    case MILLISECOND: {
                        TimeMilliVector v = (TimeMilliVector) vector;
                        IntFunction<Integer> indexedGetter = x -> {
                            long picos = longApi.getLong(x);
                            long seconds = picos / 1_000_000_000L;
                            return toIntExact(seconds);
                        };
                        copyFixedWidthVector(block, baseFixedWidthVector, parentBlock, nestedIsParent, positionCount, v::set, indexedGetter);

                        return;
                    }
                    case MICROSECOND: {
                        TimeMicroVector v = (TimeMicroVector) vector;
                        IntFunction<Long> longIntFunction = x -> {
                            long picos = longApi.getLong(x);
                            return picos / 1_000_000L;
                        };
                        copyFixedWidthVector(block, baseFixedWidthVector, parentBlock, nestedIsParent, positionCount, v::set, longIntFunction);
                        return;
                    }
                    case NANOSECOND: {
                        TimeNanoVector v = (TimeNanoVector) vector;
                        IntFunction<Long> longIntFunction = x -> {
                            long picos = longApi.getLong(x);
                            return picos / 1_000L;
                        };
                        copyFixedWidthVector(block, baseFixedWidthVector, parentBlock, nestedIsParent, positionCount, v::set, longIntFunction);

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
                copyList(trinoType, block, (ListVector) vector);
                return;
            }
            default:
                throw new UnsupportedOperationException("Unsupported Arrow type: " + arrowType);
        }
    }

    private static IntFunction<byte[]> getIndexFixedSizeByteArrayGetter(CharType trinoType, SliceBlock sliceApi)
    {
        int typeLength = trinoType.getLength();
        return x -> {
            Slice slice = sliceApi.getSlice(x);
            String charsAsPaddedString = TypeUtils.rightPadSpaces(slice.toStringUtf8(), typeLength);
            verify(CharMatcher.ascii().matchesAllOf(charsAsPaddedString), "CHAR type supports only ASCII charset");
            return charsAsPaddedString.getBytes(StandardCharsets.UTF_8);
        };
    }

    private void copyMap(Type trinoType, Block parentBlock, Block nestedBlock, MapVector vector)
    {
        LOG.debug("copyMap trinoType=%s, parentBlock=%s, nestedBlock=%s", trinoType, parentBlock.getClass(), nestedBlock.getClass());

        switch (nestedBlock) {
            case Block b when (b instanceof MapBlock || b instanceof RunLengthEncodedBlock) -> {
                int parentPositionCount = parentBlock.getPositionCount();
                // based on the arrow map vector structure: map(struct(key, value)))
                StructVector structChildVector = (StructVector) vector.getChildrenFromFields().getFirst();
                int structPositionCount;
                if (b.getPositionCount() > 0) {
                    switch (b) {
                        case MapBlock mapBlock -> {
                            SqlMap sqlMap = mapBlock.getMap(0);
                            Block keyBlock = sqlMap.getRawKeyBlock();
                            Block valueBlock = sqlMap.getRawValueBlock();

                            copyData(Optional.of(keyBlock), keyBlock, structChildVector.getChildrenFromFields().get(0));
                            copyData(Optional.of(valueBlock), valueBlock, structChildVector.getChildrenFromFields().get(1));

                            structPositionCount = keyBlock.getPositionCount();
                        }
                        case RunLengthEncodedBlock rle -> {
                            MapBlock mapBlock = (MapBlock) rle.getValue();
                            if (!mapBlock.isNull(0)) {
                                SqlMap sqlMap = mapBlock.getMap(0);
                                Block keyBlock = sqlMap.getRawKeyBlock();
                                Block valueBlock = sqlMap.getRawValueBlock();
                                Block keyRLE = RunLengthEncodedBlock.create(keyBlock, parentPositionCount);
                                Block valRLE = RunLengthEncodedBlock.create(valueBlock, parentPositionCount);
                                copyData(Optional.of(keyRLE), keyRLE, structChildVector.getChildrenFromFields().get(0));
                                copyData(Optional.of(valRLE), valRLE, structChildVector.getChildrenFromFields().get(1));
                            }
                            else {
                                MapType mapType = (MapType) trinoType;
                                Type keyType = mapType.getKeyType();
                                Type valueType = mapType.getValueType();
                                BlockBuilder keyBuilder = keyType.createBlockBuilder(null, parentBlock.getPositionCount());
                                BlockBuilder valueBuilder = valueType.createBlockBuilder(null, parentBlock.getPositionCount());
                                for (int i = 0; i < parentPositionCount; i++) {
                                    keyBuilder.appendNull();
                                    valueBuilder.appendNull();
                                }
                                Block newKeyBlock = keyBuilder.build();
                                Block newValBlock = valueBuilder.build();
                                copyData(Optional.of(newKeyBlock), newKeyBlock, structChildVector.getChildrenFromFields().get(0));
                                copyData(Optional.of(newValBlock), newValBlock, structChildVector.getChildrenFromFields().get(1));
                            }
                            structPositionCount = parentPositionCount;
                        }
                        case null, default -> throw new IllegalStateException(format("Unexpected nested block for map type: %s", nestedBlock));
                    }
                }
                else {
                    structChildVector.getChildrenFromFields().get(0).setValueCount(0);
                    structChildVector.getChildrenFromFields().get(1).setValueCount(0);
                    structPositionCount = 0;
                }
                structChildVector.setValueCount(structPositionCount);
                int validityBufferSize = BitVectorHelper.getValidityBufferSize(structPositionCount);
                try (ArrowBuf validityBuffer = allocator.buffer(validityBufferSize)) {
                    validityBuffer.setOne(0L, validityBufferSize);
                    ArrowFieldNode node = new ArrowFieldNode(structPositionCount, 0);
                    structChildVector.loadFieldBuffers(node, List.of(validityBuffer));
                }

                ColumnarMap columnarMap = ColumnarMap.toColumnarMap(nestedBlock);
                int nestedPosition = 0;
                boolean nestedIsParent = parentBlock.equals(nestedBlock);
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
            case DictionaryBlock dictionaryBlock -> {
                BlockBuilder blockBuilder = trinoType.createBlockBuilder(null, parentBlock.getPositionCount());
                MapBlock mapValueBlock = (MapBlock) dictionaryBlock.getDictionary();
                for (int i = 0; i < dictionaryBlock.getPositionCount(); i++) {
                    int position = dictionaryBlock.getId(i);
                    trinoType.appendTo(mapValueBlock, position, blockBuilder);
                }
                copyMap(trinoType, parentBlock, blockBuilder.build(), vector);
            }
            case null, default -> throw new UnsupportedOperationException(format("Unexpected block for map type: %s", nestedBlock));
        }
    }

    private void copyStruct(Block parentBlock, Block nestedBlock, StructVector sv)
    {
        RowBlock nestedRowBlock;
        IntFunction<Block> structFieldBlockFunction;
        if (nestedBlock instanceof DictionaryBlock dict) {
            LOG.debug("copyStruct got DictionaryBlock");
            nestedRowBlock = (RowBlock) dict.getDictionary();
            structFieldBlockFunction = i -> {
                int[] rawIds = dict.getRawIds();
                return nestedRowBlock.getFieldBlock(i).getPositions(rawIds, 0, dict.getPositionCount());
            };
        }
        else {
            nestedRowBlock = (RowBlock) nestedBlock;
            structFieldBlockFunction = nestedRowBlock::getFieldBlock;
        }
        List<FieldVector> subVectors = sv.getChildrenFromFields();
        int fieldCount = nestedRowBlock.getFieldBlocks().size();
        int subVectorsSize = subVectors.size();
        verify(subVectorsSize == fieldCount, format("Nested types vectors count do not match: %s/%s", subVectorsSize, fieldCount));
        for (int i = 0; i < fieldCount; i++) {
            FieldVector subVector = subVectors.get(i);
            Block rowField = structFieldBlockFunction.apply(i);
            copyData(Optional.of(parentBlock), rowField, subVector);
        }
        int positionCount = parentBlock.getPositionCount();
        try (ArrowBuf validityBuffer = allocator.buffer(BitVectorHelper.getValidityBufferSize(positionCount))) {
            int nulls = 0;
            for (int i = 0; i < positionCount; i++) {
                if (nestedBlock.isNull(i)) {
                    BitVectorHelper.unsetBit(validityBuffer, i);
                    nulls++;
                }
                else {
                    BitVectorHelper.setBit(validityBuffer, i);
                }
            }
            ArrowFieldNode node = new ArrowFieldNode(positionCount, nulls);
            sv.loadFieldBuffers(node, List.of(validityBuffer));
            sv.setValueCount(positionCount);
        }
    }

    private void copyList(Type trinoType, Block block, ListVector listVector)
    {
        FieldVector childVector = listVector.getChildrenFromFields().getFirst();
        ColumnarArray columnarArray = ColumnarArray.toColumnarArray(block);
        if (block instanceof RunLengthEncodedBlock rleBlock) {
            int rleBlockPositionCount = rleBlock.getPositionCount();
            if (rleBlockPositionCount > 0) {
                boolean isNullValue = rleBlock.isNull(0);
                if (!isNullValue) {
                    ValueBlock value = rleBlock.getValue();
                    ArrayType arrayType = (ArrayType) trinoType;
                    BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, rleBlockPositionCount);
                    for (int i = 0; i < rleBlockPositionCount; i++) {
                        arrayType.appendTo(value, 0, blockBuilder);
                    }
                    Block expandedArrayBlock = blockBuilder.build();
                    LOG.info("Array block for type %s is RLE. Copying expanded data block: %s", trinoType, expandedArrayBlock);
                    copyList(trinoType, expandedArrayBlock, listVector);
                    return; // this is a copyData over an extended Array block. Array data has been copied, no need to proceed
                }
                else {
                    LOG.info("Array block for type %s is RLE of null value", trinoType);
                    for (int i = 0; i < rleBlockPositionCount; i++) {
                        childVector.setNull(i);
                    }
                }
            } // else means empty child vector. No data copy
        }
        else {
            copyData(Optional.empty(), columnarArray.getElementsBlock(), childVector);
        }

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

    private <T> void copyTypeValues(ValueEntrySetter<T> typeSetter, boolean nestedIsParent, int positionCount, ValueEntryGetter<T> typeGetter)
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

    private <T> void copyFixedWidthVector(Block block, BaseFixedWidthVector baseFixedWidthVector, Block parentBlock,
            boolean nestedIsParent, int positionCount,
            BiConsumer<Integer, T> indexValueSetter, IntFunction<T> indexValueGetter)
    {
        baseFixedWidthVector.allocateNew(parentBlock.getPositionCount());
        copyVector(block, baseFixedWidthVector, parentBlock, nestedIsParent, positionCount, indexValueSetter, indexValueGetter);
    }

    private <T> void copyVarWidthVector(Block block, BaseVariableWidthVector varWidthVector, long totalBytes,
            Block parentBlock, boolean nestedIsParent, int positionCount,
            BiConsumer<Integer, T> indexValueSetter, IntFunction<T> indexValueGetter)
    {
        varWidthVector.allocateNew(totalBytes, positionCount);
        copyVector(block, varWidthVector, parentBlock, nestedIsParent, positionCount, indexValueSetter, indexValueGetter);
    }

    private <T> void copyVector(Block block, FieldVector varWidthVector, Block parentBlock, boolean nestedIsParent, int positionCount, BiConsumer<Integer, T> indexValueSetter, IntFunction<T> indexValueGetter)
    {
        ValueEntryGetter<T> intGetter = ValueEntryFunctionFactory.newGetter(indexValueGetter, block::isNull, parentBlock::isNull);
        ValueEntrySetter<T> intSetter = ValueEntryFunctionFactory.newSetter(indexValueSetter, varWidthVector::setNull);
        copyTypeValues(intSetter, nestedIsParent, positionCount, intGetter);
        varWidthVector.setValueCount(positionCount);
    }
}
