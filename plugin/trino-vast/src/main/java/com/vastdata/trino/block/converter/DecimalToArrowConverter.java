/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.block.converter;

import com.vastdata.trino.block.BlockApiFactory;
import com.vastdata.trino.block.LongBlockApi;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;

import java.math.BigDecimal;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.stream.IntStream;

public class DecimalToArrowConverter
        extends BlockToArrowConverter
{
    private static final java.lang.reflect.Field INT128_FIELD;
    private static final java.lang.reflect.Field LONG_FIELD;

    static {
        try {
            INT128_FIELD = Int128ArrayBlock.class.getDeclaredField("values");
            LONG_FIELD = LongArrayBlock.class.getDeclaredField("values");
            INT128_FIELD.setAccessible(true);
            LONG_FIELD.setAccessible(true);
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void convert(Type type,
                        Block block,
                        int positionCount,
                        FieldVector vector,
                        Optional<Block> optionalParent)
    {
        DecimalVector decimalVector = (DecimalVector) vector;
        decimalVector.allocateNew(positionCount);

        try {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                if (optionalParent.isEmpty()) {
                    copyShortWithoutParent(block, decimalVector, positionCount);
                }
                else {
                    copyShortWithParent(block, decimalVector, positionCount,
                            optionalParent.orElseThrow());
                }
            }
            else {
                if (optionalParent.isEmpty()) {
                    copyBigWithoutParent(type, block, decimalVector,
                            positionCount);
                }
                else {
                    copyBigWithParent(type, block, decimalVector, positionCount,
                            optionalParent.orElseThrow());
                }
            }
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void copyShortWithoutParent(Block block,
                                        DecimalVector decimalVector,
                                        int positionCount)
            throws IllegalAccessException
    {
        long[] values;
        ArrowBuf validityBuffer = decimalVector.getValidityBuffer();
        switch (block) {
            case LongArrayBlock longArrayBlock -> {
                long[] blockValues = (long[]) LONG_FIELD.get(longArrayBlock);
                values = new long[positionCount * 2];
                byte[] rawIsNull = getNulls(longArrayBlock, positionCount);
                for (int i = 0; i < positionCount; i++) {
                    if (rawIsNull[i] == 1) {
                        BitVectorHelper.unsetBit(validityBuffer, i);
                    }
                    else {
                        BitVectorHelper.setBit(validityBuffer, i);
                        values[i * 2] = blockValues[i];
                        values[i * 2 + 1] = (blockValues[i] < 0) ? -1L : 0L;
                    }
                }
            }
            case RunLengthEncodedBlock rleBlock -> {
                LongArrayBlock longArrayBlock = (LongArrayBlock) rleBlock.getSingleValueBlock(
                        0);
                if (longArrayBlock.isNull(0)) {
                    values = IntStream
                            .range(0, positionCount * 2)
                            .mapToLong(i -> 0L)
                            .toArray();
                    validityBuffer.setZero(0L,
                            BitVectorHelper.getValidityBufferSize(
                                    positionCount));
                }
                else {
                    long value = longArrayBlock.getLong(0);
                    values = new long[positionCount * 2];
                    for (int i = 0; i < positionCount; i++) {
                        values[i * 2] = value;
                        values[i * 2 + 1] = (value < 0) ? -1L : 0L;
                    }
                    validityBuffer.setOne(0L,
                            BitVectorHelper.getValidityBufferSize(
                                    positionCount));
                }
            }
            case DictionaryBlock _ -> {
                LongBlockApi longApi = BlockApiFactory.getLongApiInstance(
                        block);
                values = new long[positionCount * 2];
                if (block.mayHaveNull()) {
                    for (int i = 0; i < positionCount; i++) {
                        if (block.isNull(i)) {
                            BitVectorHelper.unsetBit(validityBuffer, i);
                        }
                        else {
                            BitVectorHelper.setBit(validityBuffer, i);
                            values[i * 2] = longApi.getLong(i);
                            values[i * 2 + 1] = (longApi.getLong(i) < 0) ?
                                    -1L :
                                    0L;
                        }
                    }
                }
                else {
                    values = new long[positionCount * 2];
                    for (int i = 0; i < positionCount; i++) {
                        values[i * 2] = longApi.getLong(i);
                        values[i * 2 + 1] = (longApi.getLong(i) < 0) ? -1L : 0L;
                    }
                    // if there are no nulls, we can set all bits at once
                    validityBuffer.setOne(0L,
                            BitVectorHelper.getValidityBufferSize(
                                    positionCount));
                }
            }
            default -> throw new IllegalStateException(
                    "Unexpected value: " + block);
        }
        decimalVector.setValueCount(positionCount);
        decimalVector
                .getDataBuffer()
                .nioBuffer(0, (int) decimalVector.getDataBuffer().capacity())
                .order(ByteOrder.LITTLE_ENDIAN)
                .asLongBuffer()
                .put(values);
    }

    private void copyShortWithParent(Block block,
                                     DecimalVector decimalVector,
                                     int positionCount,
                                     Block parentBlock)
            throws IllegalAccessException
    {
        ArrowBuf validityBuffer = decimalVector.getValidityBuffer();
        switch (block) {
            case LongArrayBlock longArrayBlock -> {
                LongBlockApi longApi = BlockApiFactory.getLongApiInstance(
                        block);
                if (!longArrayBlock.mayHaveNull() || mayBlockHaveNulls(
                        parentBlock)) {
                    byte[] rawIsNull = getNulls(longArrayBlock, positionCount);
                    for (int i = 0; i < positionCount; i++) {
                        if (rawIsNull[i] == 1 || parentBlock.isNull(i)) {
                            BitVectorHelper.unsetBit(validityBuffer, i);
                        }
                        else {
                            decimalVector.set(i, longApi.getLong(i));
                        }
                    }
                }
                else {
                    IntStream
                            .range(0, positionCount)
                            .forEach(i -> decimalVector.set(i,
                                    longApi.getLong(i)));
                }
            }
            case RunLengthEncodedBlock rleBlock -> {
                LongArrayBlock longArrayBlock = (LongArrayBlock) rleBlock.getSingleValueBlock(
                        0);
                if (longArrayBlock.isNull(0) || parentBlock.isNull(0)) {
                    validityBuffer.setZero(0L,
                            BitVectorHelper.getValidityBufferSize(
                                    positionCount));
                }
                else {
                    LongBlockApi longApi = BlockApiFactory.getLongApiInstance(
                            block);
                    long rleValue = longApi.getLong(0);
                    IntStream
                            .range(0, positionCount)
                            .forEach(i -> decimalVector.set(i, rleValue));
                }
            }
            case DictionaryBlock decimalBlock -> {
                LongBlockApi longApi = BlockApiFactory.getLongApiInstance(
                        decimalBlock);
                if (block.mayHaveNull() || mayBlockHaveNulls(parentBlock)) {
                    for (int i = 0; i < positionCount; i++) {
                        if (block.isNull(i) || parentBlock.isNull(i)) {
                            BitVectorHelper.unsetBit(validityBuffer, i);
                        }
                        else {
                            decimalVector.set(i, longApi.getLong(i));
                        }
                    }
                }
                else {
                    IntStream
                            .range(0, positionCount)
                            .forEach(i -> decimalVector.set(i,
                                    longApi.getLong(i)));
                }
            }
            default -> throw new IllegalStateException(
                    "Unexpected value: " + block);
        }
        decimalVector.setValueCount(positionCount);
    }

    private void copyBigWithParent(Type type,
                                   Block block,
                                   DecimalVector decimalVector,
                                   int positionCount,
                                   Block parentBlock)
            throws IllegalAccessException
    {
        ArrowBuf validityBuffer = decimalVector.getValidityBuffer();
        switch (block) {
            case Int128ArrayBlock int128Block -> {
                if (!int128Block.mayHaveNull() || mayBlockHaveNulls(
                        parentBlock)) {
                    byte[] rawIsNull = getNulls(int128Block, positionCount);
                    for (int i = 0; i < positionCount; i++) {
                        if (rawIsNull[i] == 1 || parentBlock.isNull(i)) {
                            BitVectorHelper.unsetBit(validityBuffer, i);
                        }
                        else {
                            BigDecimal value = Decimals.readBigDecimal(
                                    (DecimalType) type, int128Block, i);
                            decimalVector.set(i, value);
                        }
                    }
                }
                else {
                    for (int i = 0; i < positionCount; i++) {
                        BigDecimal value = Decimals.readBigDecimal(
                                (DecimalType) type, int128Block, i);
                        decimalVector.set(i, value);
                    }
                }
            }
            case RunLengthEncodedBlock rleBlock -> {
                Int128ArrayBlock int128Block = (Int128ArrayBlock) rleBlock.getSingleValueBlock(
                        0);
                if (int128Block.isNull(0) || parentBlock.isNull(0)) {
                    validityBuffer.setZero(0L,
                            BitVectorHelper.getValidityBufferSize(
                                    positionCount));
                }
                else {
                    BigDecimal rleValue = Decimals.readBigDecimal(
                            (DecimalType) type, int128Block, 0);
                    IntStream
                            .range(0, positionCount)
                            .forEach(i -> decimalVector.set(i, rleValue));
                }
            }
            case DictionaryBlock decimalBlock -> {
                if (block.mayHaveNull() || mayBlockHaveNulls(parentBlock)) {
                    for (int i = 0; i < positionCount; i++) {
                        if (block.isNull(i) || parentBlock.isNull(i)) {
                            BitVectorHelper.unsetBit(validityBuffer, i);
                        }
                        else {
                            BigDecimal value = Decimals.readBigDecimal(
                                    (DecimalType) type, decimalBlock, i);
                            decimalVector.set(i, value);
                        }
                    }
                }
                else {
                    for (int i = 0; i < positionCount; i++) {
                        BigDecimal value = Decimals.readBigDecimal(
                                (DecimalType) type, decimalBlock, i);
                        decimalVector.set(i, value);
                    }
                }
            }
            default -> throw new IllegalStateException(
                    "Unexpected value: " + block);
        }
        decimalVector.setValueCount(positionCount);
    }

    private void copyBigWithoutParent(Type type,
                                      Block block,
                                      DecimalVector decimalVector,
                                      int positionCount)
    {
        long[] values = new long[positionCount * 2];
        ArrowBuf validityBuffer = decimalVector.getValidityBuffer();
        switch (block) {
            case Int128ArrayBlock int128Block -> {
                try {
                    long[] blockValues = (long[]) INT128_FIELD.get(int128Block);
                    if (int128Block.mayHaveNull()) {
                        byte[] rawIsNull = getNulls(int128Block, positionCount);
                        for (int i = 0; i < positionCount; i++) {
                            values[i * 2] = blockValues[i * 2 + 1];
                            values[i * 2 + 1] = blockValues[i * 2];
                            if (rawIsNull[i] == 1) {
                                BitVectorHelper.unsetBit(validityBuffer, i);
                            }
                            else {
                                BitVectorHelper.setBit(validityBuffer, i);
                            }
                        }
                    }
                    else {
                        for (int i = 0; i < positionCount; i++) {
                            values[i * 2] = blockValues[i * 2 + 1];
                            values[i * 2 + 1] = blockValues[i * 2];
                        }
                        validityBuffer.setOne(0, validityBuffer.capacity());
                    }
                }
                catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
            case RunLengthEncodedBlock rleBlock -> {
                Int128ArrayBlock int128Block = (Int128ArrayBlock) rleBlock.getSingleValueBlock(
                        0);
                if (int128Block.isNull(0)) {
                    validityBuffer.setZero(0L,
                            BitVectorHelper.getValidityBufferSize(
                                    positionCount));
                }
                else {
                    BigDecimal rleValue = Decimals.readBigDecimal(
                            (DecimalType) type, int128Block, 0);
                    IntStream
                            .range(0, positionCount)
                            .forEach(i -> decimalVector.set(i, rleValue));
                }
                decimalVector.setValueCount(positionCount);
                return;
            }
            case DictionaryBlock dictBlock -> {
                if (block.mayHaveNull()) {
                    for (int i = 0; i < positionCount; i++) {
                        if (block.isNull(i)) {
                            BitVectorHelper.unsetBit(validityBuffer, i);
                        }
                        else {
                            BigDecimal value = Decimals.readBigDecimal(
                                    (DecimalType) type, dictBlock, i);
                            decimalVector.set(i, value);
                        }
                    }
                }
                else {
                    for (int i = 0; i < positionCount; i++) {
                        BigDecimal value = Decimals.readBigDecimal(
                                (DecimalType) type, dictBlock, i);
                        decimalVector.set(i, value);
                    }
                }
                decimalVector.setValueCount(positionCount);
                return;
            }
            default -> throw new IllegalStateException(
                    "Unexpected value: " + block);
        }
        decimalVector.setValueCount(positionCount);
        decimalVector
                .getDataBuffer()
                .nioBuffer(0, (int) decimalVector.getDataBuffer().capacity())
                .order(ByteOrder.LITTLE_ENDIAN)
                .asLongBuffer()
                .put(values);
    }
}
