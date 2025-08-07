/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.rowid;

import io.trino.spi.block.Block;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.LongArrayBlock;

import java.util.Comparator;
import java.util.function.Function;

public final class BlockComparatorFunction
        implements Function<Block, Comparator<Integer>>
{
    private BlockComparatorFunction() {}
    public static final BlockComparatorFunction INSTANCE = new BlockComparatorFunction();

    @Override
    public Comparator<Integer> apply(Block rowIdsBlock)
    {
        return switch (rowIdsBlock) {
            case LongArrayBlock longArrayBlock -> Comparator.comparing(longArrayBlock::getLong);
            case Int128ArrayBlock decimalRowIDsBlock -> Comparator.comparing(decimalRowIDsBlock::getInt128);
            case null -> throw new UnsupportedOperationException("Unsupported block type: null");
            default -> throw new UnsupportedOperationException("Unsupported block type " + rowIdsBlock.getClass().getName());
        };
    }
}
