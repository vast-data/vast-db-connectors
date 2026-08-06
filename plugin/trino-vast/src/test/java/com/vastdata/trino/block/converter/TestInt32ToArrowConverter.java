/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.block.converter;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.vastdata.trino.TypeUtils.TYPE_OPERATORS;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestInt32ToArrowConverter
{
    private Int32ToArrowConverter converter;
    private RootAllocator allocator;
    private IntVector vector;

    @BeforeMethod
    public void setUp()
    {
        this.converter = new Int32ToArrowConverter();
        this.allocator = new RootAllocator();
        Field field = new Field("test",
                org.apache.arrow.vector.types.pojo.FieldType.nullable(
                        new ArrowType.Int(32, true)), ImmutableList.of());
        this.vector = new IntVector(field, this.allocator);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        this.vector.close();
        this.allocator.close();
    }

    @Test
    public void testIntArrayBlockWithNull()
    {
        Block block = new IntArrayBlock(4,
                Optional.of(nulls(false, false, true, false)),
                new int[] {1, 2, 0, 4});
        this.converter.convert(IntegerType.INTEGER, block, 4, vector,
                Optional.empty());
        assertThat(vector.getValueCount()).isEqualTo(4);
        assertThat(vector.isNull(0)).isFalse();
        assertThat(vector.get(0)).isEqualTo(1);
        assertThat(vector.isNull(1)).isFalse();
        assertThat(vector.get(1)).isEqualTo(2);
        assertThat(vector.isNull(2)).isTrue();
        assertThat(vector.isNull(3)).isFalse();
        assertThat(vector.get(3)).isEqualTo(4);
    }

    @Test
    public void testDictionaryBlockWithNull()
    {
        IntArrayBlock dictionary = new IntArrayBlock(4,
                Optional.of(new boolean[] {false, false, true, false}),
                new int[] {1, 2, 0, 4});
        Block block = DictionaryBlock.create(4, dictionary,
                new int[] {0, 1, 2, 3});
        this.converter.convert(IntegerType.INTEGER, block, 4, vector,
                Optional.empty());
        assertThat(vector.getValueCount()).isEqualTo(4);
        assertThat(vector.isNull(0)).isFalse();
        assertThat(vector.get(0)).isEqualTo(1);
        assertThat(vector.isNull(1)).isFalse();
        assertThat(vector.get(1)).isEqualTo(2);
        assertThat(vector.isNull(2)).isTrue();
        assertThat(vector.isNull(3)).isFalse();
        assertThat(vector.get(3)).isEqualTo(4);
    }

    @Test
    public void testRLEBlock()
    {
        RunLengthEncodedBlock block = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(
                IntegerType.INTEGER, 1L, 4);
        this.converter.convert(IntegerType.INTEGER, block, 4, vector,
                Optional.empty());
        assertThat(vector.getValueCount()).isEqualTo(4);
        assertThat(vector.get(0)).isEqualTo(1);
        assertThat(vector.get(3)).isEqualTo(1);
    }

    @Test
    public void testIntArrayBlockWithNullWithParent()
    {
        Block block = new IntArrayBlock(4,
                Optional.of(nulls(false, false, true, false)),
                new int[] {1, 2, 0, 4});
        Block parentBlock = RowBlock.fromNotNullSuppressedFieldBlocks(4,
                Optional.of(nulls(false, false, false, false)),
                new Block[] {block});
        this.converter.convert(IntegerType.INTEGER, block, 4, vector,
                Optional.of(parentBlock));
        assertThat(vector.getValueCount()).isEqualTo(4);
        assertThat(vector.isNull(0)).isFalse();
        assertThat(vector.get(0)).isEqualTo(1);
        assertThat(vector.isNull(1)).isFalse();
        assertThat(vector.get(1)).isEqualTo(2);
        assertThat(vector.isNull(2)).isTrue();
        assertThat(vector.isNull(3)).isFalse();
        assertThat(vector.get(3)).isEqualTo(4);
    }

    @Test
    public void testDictionaryBlockWithNullWithParent()
    {
        IntArrayBlock dictionary = new IntArrayBlock(4,
                Optional.of(new boolean[] {false, false, true, false}),
                new int[] {1, 2, 0, 4});
        Block block = DictionaryBlock.create(4, dictionary,
                new int[] {0, 1, 2, 3});
        Block parentBlock = RowBlock.fromNotNullSuppressedFieldBlocks(4,
                Optional.of(nulls(false, false, false, false)),
                new Block[] {block});
        this.converter.convert(IntegerType.INTEGER, block, 4, vector,
                Optional.of(parentBlock));
        assertThat(vector.getValueCount()).isEqualTo(4);
        assertThat(vector.isNull(0)).isFalse();
        assertThat(vector.get(0)).isEqualTo(1);
        assertThat(vector.isNull(1)).isFalse();
        assertThat(vector.get(1)).isEqualTo(2);
        assertThat(vector.isNull(2)).isTrue();
        assertThat(vector.isNull(3)).isFalse();
        assertThat(vector.get(3)).isEqualTo(4);
    }

    @Test
    public void testRLEBlockWithParent()
    {
        RunLengthEncodedBlock block = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(
                IntegerType.INTEGER, 1L, 4);
        RunLengthEncodedBlock parentBlock = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(
                IntegerType.INTEGER, null, 4);
        this.converter.convert(IntegerType.INTEGER, block, 4, vector,
                Optional.of(parentBlock));
        assertThat(vector.getValueCount()).isEqualTo(4);
        assertThat(vector.isNull(0)).isTrue();
        assertThat(vector.isNull(3)).isTrue();
    }

    @Test
    public void testWithMapParent()
    {
        Block keyBlock = new IntArrayBlock(4,
                Optional.of(nulls(false, false, false, false)),
                new int[] {1, 2, 3, 4});
        Block valueBlock = new IntArrayBlock(4,
                Optional.of(nulls(false, true, false, true)),
                new int[] {0, 0, 0, 40});
        MapType mapType = new MapType(IntegerType.INTEGER, IntegerType.INTEGER,
                TYPE_OPERATORS);

        Block parentBlock = MapBlock.fromKeyValueBlock(
                Optional.of(nulls(false, true, false, true)),
                new int[] {0, 1, 2, 3, 4}, keyBlock, valueBlock, mapType);

        Block block = new IntArrayBlock(4,
                Optional.of(nulls(false, false, false, false)),
                new int[] {1, 2, 3, 4});
        this.converter.convert(IntegerType.INTEGER, block, 4, vector,
                Optional.of(parentBlock));
        assertThat(vector.getValueCount()).isEqualTo(4);
        assertThat(vector.isNull(0)).isFalse();
        assertThat(vector.get(0)).isEqualTo(1);
        assertThat(vector.isNull(1)).isTrue();
        assertThat(vector.isNull(2)).isFalse();
        assertThat(vector.get(2)).isEqualTo(3);
        assertThat(vector.isNull(3)).isTrue();
    }

    @Test
    public void testAllNulls()
    {
        Block block = new IntArrayBlock(4,
                Optional.of(nulls(true, true, true, true)),
                new int[] {0, 0, 0, 0});
        this.converter.convert(IntegerType.INTEGER, block, 4, vector,
                Optional.empty());
        assertThat(vector.getValueCount()).isEqualTo(4);
        assertThat(vector.isNull(0)).isTrue();
        assertThat(vector.isNull(3)).isTrue();
    }

    private static boolean[] nulls(boolean... nulls)
    {
        return nulls;
    }
}
