/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.block.arrowconverter;

import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.FieldVector;

import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class TimeArrowToBlockConverter
{
    public Block convert(Type type,
                         List<FieldVector> vectors,
                         int positions,
                         Optional<boolean[]> parentVectorIsNull)
    {
        Block ret;
        TimeType timeType = (TimeType) type;
        long precisionFactor = (long) Math.pow(10,
                12 - timeType.getPrecision());
        boolean useInt = timeType.getPrecision() <= 3;
        if (parentVectorIsNull.isPresent()) {
            if (useInt) {
                ret = copyIntTimeWithParent(vectors, positions,
                        parentVectorIsNull.orElseThrow(), precisionFactor);
            }
            else {
                ret = copyLongTimeWithParent(vectors, positions,
                        parentVectorIsNull.orElseThrow(), precisionFactor);
            }
        }
        else {
            if (useInt) {
                ret = copyIntTime(vectors, positions, precisionFactor);
            }
            else {
                ret = copyLongTime(vectors, positions, precisionFactor);
            }
        }
        return ret;
    }

    private Block copyIntTimeWithParent(List<FieldVector> vectors,
                                        int positions,
                                        boolean[] parentVectorIsNull,
                                        long factor)
    {
        long[] values = new long[positions];
        boolean[] isNull = new boolean[positions];
        int totalPositions = 0;
        for (FieldVector vector : vectors) {
            IntBuffer buffer = vector
                    .getDataBuffer()
                    .nioBuffer()
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .asIntBuffer();
            byte[] vectorIsNullValue = new byte[(int) Math.ceil(
                    (double) vector.getValueCount() / 8)];
            vector.getValidityBuffer().getBytes(0, vectorIsNullValue);
            for (int i = 0; i < vector.getValueCount(); ++i) {
                if (parentVectorIsNull[totalPositions + i]) {
                    isNull[totalPositions + i] = true;
                }
                else {
                    if ((vectorIsNullValue[i / 8] & (1 << (i % 8))) != 0) {
                        values[totalPositions + i] = buffer.get(i) * factor;
                        isNull[totalPositions + i] = false;
                    }
                    else {
                        isNull[totalPositions + i] = true;
                    }
                }
            }
            totalPositions += vector.getValueCount();
        }
        return new LongArrayBlock(positions, Optional.of(isNull), values);
    }

    private Block copyLongTimeWithParent(List<FieldVector> vectors,
                                         int positions,
                                         boolean[] parentVectorIsNull,
                                         long factor)
    {
        long[] values = new long[positions];
        boolean[] isNull = new boolean[positions];
        int totalPositions = 0;
        for (FieldVector vector : vectors) {
            LongBuffer buffer = vector
                    .getDataBuffer()
                    .nioBuffer()
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .asLongBuffer();
            byte[] vectorIsNullValue = new byte[(int) Math.ceil(
                    (double) vector.getValueCount() / 8)];
            vector.getValidityBuffer().getBytes(0, vectorIsNullValue);
            for (int i = 0; i < vector.getValueCount(); ++i) {
                if (parentVectorIsNull[totalPositions + i]) {
                    isNull[totalPositions + i] = true;
                }
                else {
                    if ((vectorIsNullValue[i / 8] & (1 << (i % 8))) != 0) {
                        values[totalPositions + i] = buffer.get(i) * factor;
                        isNull[totalPositions + i] = false;
                    }
                    else {
                        isNull[totalPositions + i] = true;
                    }
                }
            }
            totalPositions += vector.getValueCount();
        }
        return new LongArrayBlock(positions, Optional.of(isNull), values);
    }

    private Block copyLongTime(List<FieldVector> vectors,
                               int positions,
                               long factor)
    {
        long[] values = new long[positions];
        boolean[] isNull = null;
        int totalPositions = 0;
        for (FieldVector vector : vectors) {
            if (vector.getNullCount() == 0) {
                LongBuffer longBuffer = vector
                        .getDataBuffer()
                        .nioBuffer()
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .asLongBuffer();
                long[] vectorLongValues = new long[vector.getValueCount()];
                longBuffer.get(0, vectorLongValues, 0, vector.getValueCount());
                long[] vectorValues = Arrays
                        .stream(vectorLongValues)
                        .map(l -> l * factor)
                        .toArray();
                System.arraycopy(vectorValues, 0, values, totalPositions,
                        vector.getValueCount());
            }
            else {
                isNull = new boolean[positions];
                LongBuffer buffer = vector
                        .getDataBuffer()
                        .nioBuffer()
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .asLongBuffer();
                byte[] vectorIsNullValue = new byte[(int) Math.ceil(
                        (double) vector.getValueCount() / 8)];
                vector.getValidityBuffer().getBytes(0, vectorIsNullValue);
                for (int i = 0; i < vector.getValueCount(); ++i) {
                    if ((vectorIsNullValue[i / 8] & (1 << (i % 8))) != 0) {
                        values[totalPositions + i] = buffer.get(i) * factor;
                        isNull[totalPositions + i] = false;
                    }
                    else {
                        isNull[totalPositions + i] = true;
                    }
                }
            }
            totalPositions += vector.getValueCount();
        }
        return new LongArrayBlock(positions, Optional.ofNullable(isNull),
                values);
    }

    private Block copyIntTime(List<FieldVector> vectors,
                              int positions,
                              long factor)
    {
        long[] values = new long[positions];
        boolean[] isNull = null;
        int totalPositions = 0;
        for (FieldVector vector : vectors) {
            if (vector.getNullCount() == 0) {
                IntBuffer longBuffer = vector
                        .getDataBuffer()
                        .nioBuffer()
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .asIntBuffer();
                int[] vectorIntValues = new int[vector.getValueCount()];
                longBuffer.get(0, vectorIntValues, 0, vector.getValueCount());
                long[] vectorValues = Arrays
                        .stream(vectorIntValues)
                        .mapToLong(i -> (long) i)
                        .map(l -> l * factor)
                        .toArray();
                System.arraycopy(vectorValues, 0, values, totalPositions,
                        vector.getValueCount());
            }
            else {
                isNull = new boolean[positions];
                IntBuffer buffer = vector
                        .getDataBuffer()
                        .nioBuffer()
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .asIntBuffer();
                byte[] vectorIsNullValue = new byte[(int) Math.ceil(
                        (double) vector.getValueCount() / 8)];
                vector.getValidityBuffer().getBytes(0, vectorIsNullValue);
                for (int i = 0; i < vector.getValueCount(); ++i) {
                    if ((vectorIsNullValue[i / 8] & (1 << (i % 8))) != 0) {
                        values[totalPositions + i] = buffer.get(i) * factor;
                        isNull[totalPositions + i] = false;
                    }
                    else {
                        isNull[totalPositions + i] = true;
                    }
                }
            }
            totalPositions += vector.getValueCount();
        }
        return new LongArrayBlock(positions, Optional.ofNullable(isNull),
                values);
    }
}
