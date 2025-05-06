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
        if (rowIdsBlock instanceof LongArrayBlock longArrayBlock) {
            return Comparator.comparing(longArrayBlock::getLong);
        }
        else if (rowIdsBlock instanceof Int128ArrayBlock decimalRowIDsBlock) {
            return Comparator.comparing(decimalRowIDsBlock::getInt128);
        }
        else {
            throw new UnsupportedOperationException("Unsupported block type " + rowIdsBlock.getClass().getName());
        }
    }
}
