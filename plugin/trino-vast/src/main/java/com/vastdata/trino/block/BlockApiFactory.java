/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.block;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.Fixed12Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlock;

import java.util.function.Function;

import static java.lang.String.format;

public final class BlockApiFactory
{
    private BlockApiFactory() {}

    public static SliceBlock getSliceApiInstance(Block block)
    {
        return switch (block) {
            case VariableWidthBlock vwb -> new SliceApiBlockWrapper(vwb::getSlice, vwb::getSliceLength);
            case RunLengthEncodedBlock rle -> {
                ValueBlock value = rle.getValue();
                if (value instanceof VariableWidthBlock wrapped) {
                    yield new ConstantSliceApi(wrapped.getSlice(0));
                }
                else {
                    throw new RuntimeException(format("Unexpected nested block for Slice RunLengthEncodedBlock: %s", value.getClass()));
                }
            }
            case DictionaryBlock dict -> {
                if (dict.getUnderlyingValueBlock() instanceof VariableWidthBlock) {
                    Function<Integer, Slice> dictIdSliceFunction = i -> {
                        int newPosition = dict.getId(i);
                        return ((VariableWidthBlock) dict.getUnderlyingValueBlock()).getSlice(newPosition);
                    };
                    Function<Integer, Integer> dictIdIntFunction = i -> ((VariableWidthBlock) dict.getUnderlyingValueBlock()).getSliceLength(dict.getId(i));
                    yield new SliceApiBlockWrapper(dictIdSliceFunction, dictIdIntFunction);
                }
                else {
                    throw new RuntimeException(format("Unexpected nested block for Slice DictionaryBlock: %s", dict.getUnderlyingValueBlock().getClass()));
                }
            }
            default -> throw new IllegalStateException(format("Unexpected block class for Slice api: %s", block));
        };
    }

    public static IntBlockApi getIntApiInstance(Block block)
    {
        return switch (block) {
            case IntArrayBlock b -> new IntApiBlockWrapper(b::getInt);
            case RunLengthEncodedBlock rle -> {
                ValueBlock value = rle.getValue();
                if (value instanceof IntArrayBlock wrapped) {
                    yield new ConstantIntApi(wrapped.getInt(0));
                }
                else {
                    throw new RuntimeException(format("Unexpected nested block for Int RunLengthEncodedBlock: %s", value.getClass()));
                }
            }
            case DictionaryBlock dict -> {
                if (dict.getUnderlyingValueBlock() instanceof IntArrayBlock) {
                    Function<Integer, Integer> dictIdIntFunction = i ->
                            ((IntArrayBlock) dict.getUnderlyingValueBlock()).getInt(dict.getId(i));
                    yield new IntApiBlockWrapper(dictIdIntFunction);
                }
                else {
                    throw new RuntimeException(format("Unexpected nested block for Int DictionaryBlock: %s", dict.getUnderlyingValueBlock().getClass()));
                }
            }
            default -> throw new IllegalStateException(format("Unexpected block class for Int api: %s", block));
        };
    }

    public static LongBlockApi getLongApiInstance(Block block)
    {
        return switch (block) {
            case LongArrayBlock b -> new LongApiBlockWrapper(b::getLong);
            case RunLengthEncodedBlock rle -> {
                ValueBlock value = rle.getValue();
                if (value instanceof LongArrayBlock wrapped) {
                    yield new ConstantLongApi(wrapped.getLong(0));
                }
                else {
                    throw new RuntimeException(format("Unexpected nested block for Long RunLengthEncodedBlock: %s", value.getClass()));
                }
            }
            case DictionaryBlock dict -> {
                if (dict.getUnderlyingValueBlock() instanceof LongArrayBlock) {
                    Function<Integer, Long> dictIdLongFunction = i ->
                            ((LongArrayBlock) dict.getUnderlyingValueBlock()).getLong(dict.getId(i));
                    yield new LongApiBlockWrapper(dictIdLongFunction);
                }
                else {
                    throw new RuntimeException(format("Unexpected nested block for Long DictionaryBlock: %s", dict.getUnderlyingValueBlock().getClass()));
                }
            }
            default -> throw new IllegalStateException(format("Unexpected block class for Long api: %s", block));
        };
    }

    public static ShortBlockApi getShortApiInstance(Block block)
    {
        return switch (block) {
            case ShortArrayBlock b -> new ShortApiBlockWrapper(b::getShort);
            case RunLengthEncodedBlock rle -> {
                ValueBlock value = rle.getValue();
                if (value instanceof ShortArrayBlock wrapped) {
                    yield new ConstantShortApi(wrapped.getShort(0));
                }
                else {
                    throw new RuntimeException(format("Unexpected nested block for Short RunLengthEncodedBlock: %s", value.getClass()));
                }
            }
            case DictionaryBlock dict -> {
                if (dict.getUnderlyingValueBlock() instanceof ShortArrayBlock) {
                    Function<Integer, Short> dictIdShortFunction = i ->
                            ((ShortArrayBlock) dict.getUnderlyingValueBlock()).getShort(dict.getId(i));
                    yield new ShortApiBlockWrapper(dictIdShortFunction);
                }
                else {
                    throw new RuntimeException(format("Unexpected nested block for Short DictionaryBlock: %s", dict.getUnderlyingValueBlock().getClass()));
                }
            }
            default -> throw new IllegalStateException(format("Unexpected block class for Short api: %s", block));
        };
    }

    public static ByteBlockApi getByteApiInstance(Block block)
    {
        return switch (block) {
            case ByteArrayBlock b -> new ByteApiBlockWrapper(b::getByte);
            case RunLengthEncodedBlock rle -> {
                ValueBlock value = rle.getValue();
                if (value instanceof ByteArrayBlock wrapped) {
                    yield new ConstantByteApi(wrapped.getByte(0));
                }
                else {
                    throw new RuntimeException(format("Unexpected nested block for Byte RunLengthEncodedBlock: %s", value.getClass()));
                }
            }
            case DictionaryBlock dict -> {
                if (dict.getUnderlyingValueBlock() instanceof ByteArrayBlock) {
                    Function<Integer, Byte> dictIdByteFunction = i ->
                            ((ByteArrayBlock) dict.getUnderlyingValueBlock()).getByte(dict.getId(i));
                    yield new ByteApiBlockWrapper(dictIdByteFunction);
                }
                else {
                    throw new RuntimeException(format("Unexpected nested block for Byte DictionaryBlock: %s", dict.getUnderlyingValueBlock().getClass()));
                }
            }
            default -> throw new IllegalStateException(format("Unexpected block class for Byte api: %s", block));
        };
    }

    public static Fixed12BlockApi getFixed12ApiInstance(Block block)
    {
        return switch (block) {
            case Fixed12Block b -> new Fixed12ApiBlockWrapper(b::getFixed12First, b::getFixed12Second);
            case RunLengthEncodedBlock rle -> {
                ValueBlock value = rle.getValue();
                if (value instanceof Fixed12Block wrapped) {
                    yield new ConstantFixed12Api(wrapped.getFixed12First(0), wrapped.getFixed12Second(0));
                }
                else {
                    throw new RuntimeException(format("Unexpected nested block for Fixed12 RunLengthEncodedBlock: %s", value.getClass()));
                }
            }
            case DictionaryBlock dict -> {
                if (dict.getUnderlyingValueBlock() instanceof Fixed12Block) {
                    Function<Integer, Long> dictIdFirstFunction = i -> ((Fixed12Block) dict.getUnderlyingValueBlock()).getFixed12First(dict.getId(i));
                    Function<Integer, Integer> dictIdSecondFunction = i -> ((Fixed12Block) dict.getUnderlyingValueBlock()).getFixed12Second(dict.getId(i));
                    yield new Fixed12ApiBlockWrapper(dictIdFirstFunction, dictIdSecondFunction);
                }
                else {
                    throw new RuntimeException(format("Unexpected nested block for Fixed12 DictionaryBlock: %s", dict.getUnderlyingValueBlock().getClass()));
                }
            }
            default -> throw new IllegalStateException(format("Unexpected block class for Fixed12 api: %s", block));
        };
    }

    public static Int128ArrayBlockApi getInt128ApiInstance(Block block)
    {
        return switch (block) {
            case Int128ArrayBlock b -> new Int128ApiBlockWrapper(b::getInt128High, b::getInt128Low);
            case RunLengthEncodedBlock rle -> {
                ValueBlock value = rle.getValue();
                if (value instanceof Int128ArrayBlock wrapped) {
                    yield new ConstantInt128ArrayApi(wrapped.getInt128High(0), wrapped.getInt128Low(0));
                }
                else {
                    throw new RuntimeException(format("Unexpected nested block for Int128 RunLengthEncodedBlock: %s", value.getClass()));
                }
            }
            case DictionaryBlock dict -> {
                if (dict.getUnderlyingValueBlock() instanceof Int128ArrayBlock) {
                    Function<Integer, Long> dictIdFirstFunction = i -> ((Int128ArrayBlock) dict.getUnderlyingValueBlock()).getInt128High(dict.getId(i));
                    Function<Integer, Long> dictIdSecondFunction = i -> ((Int128ArrayBlock) dict.getUnderlyingValueBlock()).getInt128Low(dict.getId(i));
                    yield new Int128ApiBlockWrapper(dictIdFirstFunction, dictIdSecondFunction);
                }
                else {
                    throw new RuntimeException(format("Unexpected nested block for Int128 DictionaryBlock: %s", dict.getUnderlyingValueBlock().getClass()));
                }
            }
            default -> throw new IllegalStateException(format("Unexpected block class for Int128 api: %s", block));
        };
    }
}
