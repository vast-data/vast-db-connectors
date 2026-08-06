/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.block.converter;

import com.vastdata.trino.block.BlockApiFactory;
import com.vastdata.trino.block.IntBlockApi;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;

import java.nio.ByteOrder;
import java.util.Optional;
import java.util.stream.IntStream;

import static java.lang.String.format;

public class Int32ToArrowConverter
        extends BlockToArrowConverter
{
    @Override
    public void convert(Type type,
                        Block block,
                        int position,
                        FieldVector vector,
                        Optional<Block> optionalParent)
    {
        IntVector fieldVector = (IntVector) vector;
        fieldVector.allocateNew(block.getPositionCount());
        if (optionalParent.isEmpty()) {
            copyIntWithoutParent(block, fieldVector, position);
        }
        else {
            copyIntWithParent(block, fieldVector, position,
                    optionalParent.orElseThrow());
        }
    }

    private void copyIntWithParent(Block block,
                                   IntVector baseIntVector,
                                   int positionCount,
                                   Block parentBlock)
    {
        ArrowBuf validityBuffer = baseIntVector.getValidityBuffer();
        baseIntVector.setValueCount(positionCount);
        switch (block) {
            case IntArrayBlock intBlock -> {
                int[] rawValues = intBlock.getRawValues();
                if (intBlock.mayHaveNull() || mayBlockHaveNulls(parentBlock)) {
                    byte[] rawIsNull = getNulls(intBlock, positionCount);
                    for (int i = 0; i < positionCount; i++) {
                        if (rawIsNull[i] == 1 || parentBlock.isNull(i)) {
                            BitVectorHelper.unsetBit(validityBuffer, i);
                        }
                        else {
                            baseIntVector.set(i, rawValues[i]);
                        }
                    }
                }
                else {
                    IntStream
                            .range(0, positionCount)
                            .forEach(i -> baseIntVector.set(i, rawValues[i]));
                }
            }
            case RunLengthEncodedBlock runLengthEncodedBlock -> {
                IntArrayBlock intBlock = (IntArrayBlock) runLengthEncodedBlock.getSingleValueBlock(
                        0);
                if (!intBlock.mayHaveNull() && !mayBlockHaveNulls(
                        parentBlock)) {
                    int rleValue = intBlock.getInt(0);
                    IntStream
                            .range(0, positionCount)
                            .forEach(i -> baseIntVector.set(i, rleValue));
                }
                else {
                    validityBuffer.setZero(0L,
                            BitVectorHelper.getValidityBufferSize(
                                    positionCount));
                }
            }
            case DictionaryBlock _ -> {
                IntBlockApi intApi = BlockApiFactory.getIntApiInstance(block);
                if (block.mayHaveNull() || mayBlockHaveNulls(parentBlock)) {
                    for (int i = 0; i < positionCount; i++) {
                        if (block.isNull(i)) {
                            baseIntVector.setNull(i);
                        }
                        else {
                            baseIntVector.set(i, intApi.getInt(i));
                        }
                    }
                }
                else {
                    IntStream
                            .range(0, positionCount)
                            .forEach(i -> baseIntVector.set(i,
                                    intApi.getInt(i)));
                }
            }
            default -> throw new UnsupportedOperationException(
                    format("Unsupported block type for int vector: %s",
                            block.getClass()));
        }
    }

    private void copyIntWithoutParent(Block block,
                                      BaseFixedWidthVector baseIntVector,
                                      int positionCount)
    {
        ArrowBuf validityBuffer = baseIntVector.getValidityBuffer();
        int[] rawValues;
        switch (block) {
            case IntArrayBlock intBlock -> {
                rawValues = intBlock.getRawValues();
                if (intBlock.mayHaveNull()) {
                    byte[] rawIsNull = getNulls(intBlock, positionCount);
                    for (int i = 0; i < positionCount; i++) {
                        if (rawIsNull[i] == 1) {
                            BitVectorHelper.unsetBit(validityBuffer, i);
                        }
                        else {
                            BitVectorHelper.setBit(validityBuffer, i);
                        }
                    }
                }
                else {
                    // if there are no nulls, we can set all bits at once
                    validityBuffer.setOne(0L,
                            BitVectorHelper.getValidityBufferSize(
                                    positionCount));
                }
            }
            case RunLengthEncodedBlock runLengthEncodedBlock -> {
                IntArrayBlock intBlock = (IntArrayBlock) runLengthEncodedBlock.getSingleValueBlock(
                        0);
                if (intBlock.isNull(0)) {
                    rawValues = IntStream
                            .range(0, positionCount)
                            .map(i -> 0)
                            .toArray();
                    validityBuffer.setZero(0L,
                            BitVectorHelper.getValidityBufferSize(
                                    positionCount));
                }
                else {
                    int rleValue = intBlock.getInt(0);
                    rawValues = IntStream
                            .range(0, positionCount)
                            .map(i -> rleValue)
                            .toArray();
                    validityBuffer.setOne(0L,
                            BitVectorHelper.getValidityBufferSize(
                                    positionCount));
                }
            }
            case DictionaryBlock _ -> {
                IntBlockApi intApi = BlockApiFactory.getIntApiInstance(block);
                rawValues = new int[positionCount];
                if (block.mayHaveNull()) {
                    for (int i = 0; i < positionCount; i++) {
                        if (block.isNull(i)) {
                            BitVectorHelper.unsetBit(validityBuffer, i);
                        }
                        else {
                            BitVectorHelper.setBit(validityBuffer, i);
                            rawValues[i] = intApi.getInt(i);
                        }
                    }
                }
                else {
                    for (int i = 0; i < positionCount; i++) {
                        rawValues[i] = intApi.getInt(i);
                    }
                    // if there are no nulls, we can set all bits at once
                    validityBuffer.setOne(0L,
                            BitVectorHelper.getValidityBufferSize(
                                    positionCount));
                }
            }
            default -> throw new UnsupportedOperationException(
                    format("Unsupported block type for int vector: %s",
                            block.getClass()));
        }
        baseIntVector.setValueCount(positionCount);
        baseIntVector
                .getDataBuffer()
                .nioBuffer()
                .order(ByteOrder.LITTLE_ENDIAN)
                .asIntBuffer()
                .put(rawValues, 0, positionCount);
    }
}
