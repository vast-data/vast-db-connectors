/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.block.converter;

import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.FieldVector;

import java.util.Arrays;
import java.util.Optional;

import static java.lang.String.format;

public abstract class BlockToArrowConverter
{
    public abstract void convert(Type type,
                                 Block block,
                                 int position,
                                 FieldVector vector,
                                 Optional<Block> optionalParent);

    protected boolean mayBlockHaveNulls(Block block)
    {
        return switch (block) {
            case IntArrayBlock intBlock -> intBlock.mayHaveNull();
            case LongArrayBlock longBlock -> longBlock.mayHaveNull();
            case Int128ArrayBlock int128Block -> int128Block.mayHaveNull();
            case RunLengthEncodedBlock runLengthEncodedBlock ->
                    mayBlockHaveNulls(
                            runLengthEncodedBlock.getSingleValueBlock(0));
            case DictionaryBlock dictionaryBlock ->
                    dictionaryBlock.mayHaveNull();
            case RowBlock rowBlock -> rowBlock.mayHaveNull();
            case MapBlock mapBlock -> mapBlock.mayHaveNull();
            default -> throw new UnsupportedOperationException(
                    format("Unsupported block type for int vector: %s",
                            block.getClass()));
        };
    }

    protected byte[] getNulls(ValueBlock block, int positionCount)
    {
        Optional<ByteArrayBlock> nullsOpt = block.getNulls();
        if (nullsOpt.isPresent()) {
            return nullsOpt.orElseThrow().getRawValues();
        }
        else {
            byte[] ret = new byte[positionCount];
            Arrays.fill(ret, (byte) 0);
            return ret;
        }
    }
}
