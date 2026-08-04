/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.block.converter;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestDecimalToArrowConverter
{
    private static final DecimalType DECIMAL_TYPE = DecimalType.createDecimalType(
            38, 10);
    private DecimalToArrowConverter converter;
    private RootAllocator allocator;
    private DecimalVector vector;
    private DecimalType type;

    @BeforeMethod
    public void setUp()
    {
        this.converter = new DecimalToArrowConverter();
        this.allocator = new RootAllocator();
        Field field = new Field("test",
                FieldType.nullable(new ArrowType.Decimal(38, 10, 128)),
                ImmutableList.of());
        this.vector = new DecimalVector(field, this.allocator);
        this.type = DecimalType.createDecimalType(38, 10);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        this.vector.close();
        this.allocator.close();
    }

    @Test
    public void testInt128ArrayBlockWithNull()
    {
        Block block = new Int128ArrayBlock(4,
                Optional.of(new boolean[] {false, false, true, false}),
                new long[] {0L, 1L, 0L, 2L, 0L, 0L, 0L, 4L});
        this.converter.convert(type, block, 4, vector, Optional.empty());
        assertThat(vector.getValueCount()).isEqualTo(4);
        assertThat(vector.isNull(0)).isFalse();
        LongBuffer buffer = vector.getDataBuffer().nioBuffer().order(
                ByteOrder.LITTLE_ENDIAN).asLongBuffer();
        assertThat(buffer.get(0)).isEqualTo(1L);
        assertThat(buffer.get(1)).isEqualTo(0L);

        assertThat(vector.isNull(1)).isFalse();
        assertThat(buffer.get(2)).isEqualTo(2L);
        assertThat(buffer.get(3)).isEqualTo(0L);
        assertThat(vector.isNull(3)).isFalse();
        assertThat(buffer.get(6)).isEqualTo(4L);
        assertThat(buffer.get(7)).isEqualTo(0L);
    }

    @Test
    public void testDictionaryBlockWithNull()
    {
        Int128ArrayBlock dictionary = new Int128ArrayBlock(4,
                Optional.of(new boolean[] {false, false, true, false}),
                new long[] {0L, 1L, 0L, 2L, 0L, 0L, 0L, 4L});
        Block block = DictionaryBlock.create(4, dictionary,
                new int[] {0, 1, 2, 3});
        this.converter.convert(type, block, 4, vector, Optional.empty());
        assertThat(vector.getValueCount()).isEqualTo(4);
        LongBuffer buffer = vector.getDataBuffer().nioBuffer().order(
                ByteOrder.LITTLE_ENDIAN).asLongBuffer();
        assertThat(buffer.get(0)).isEqualTo(1L);
        assertThat(buffer.get(1)).isEqualTo(0L);

        assertThat(vector.isNull(1)).isFalse();
        assertThat(buffer.get(2)).isEqualTo(2L);
        assertThat(buffer.get(3)).isEqualTo(0L);
        assertThat(vector.isNull(3)).isFalse();
        assertThat(buffer.get(6)).isEqualTo(4L);
        assertThat(buffer.get(7)).isEqualTo(0L);
    }

    @Test
    public void testRLEBlock()
    {
        RunLengthEncodedBlock block = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(
                DECIMAL_TYPE,
                Decimals.encodeScaledValue(new BigDecimal("1.0"), 1), 4);
        this.converter.convert(type, block, 4, vector, Optional.empty());
        assertThat(vector.getValueCount()).isEqualTo(4);
        LongBuffer buffer = vector.getDataBuffer().nioBuffer().order(
                ByteOrder.LITTLE_ENDIAN).asLongBuffer();
        assertThat(buffer.get(0)).isEqualTo(0L);
        assertThat(buffer.get(1)).isEqualTo(10L);
        assertThat(buffer.get(6)).isEqualTo(0L);
        assertThat(buffer.get(7)).isEqualTo(10L);
    }

    @Test
    public void testInt128ArrayBlockWithNullWithParent()
    {
        Int128ArrayBlock block = new Int128ArrayBlock(4,
                Optional.of(new boolean[] {false, false, true, false}),
                new long[] {0L, 1L, 0L, 2L, 0L, 0L, 0L, 4L});
        Block parentBlock = RowBlock.fromNotNullSuppressedFieldBlocks(4,
                Optional.of(new boolean[] {false, false, false, false}),
                new Block[] {block});
        this.converter.convert(type, block, 4, vector,
                Optional.of(parentBlock));
        assertThat(vector.getValueCount()).isEqualTo(4);
        LongBuffer buffer = vector.getDataBuffer().nioBuffer().order(
                ByteOrder.LITTLE_ENDIAN).asLongBuffer();
        assertThat(buffer.get(0)).isEqualTo(1L);
        assertThat(buffer.get(1)).isEqualTo(0L);

        assertThat(vector.isNull(1)).isFalse();
        assertThat(buffer.get(2)).isEqualTo(2L);
        assertThat(buffer.get(3)).isEqualTo(0L);
        assertThat(vector.isNull(3)).isFalse();
        assertThat(buffer.get(6)).isEqualTo(4L);
        assertThat(buffer.get(7)).isEqualTo(0L);
    }

    @Test
    public void testNegativeBigDecimalInDictionary()
    {
        BigDecimal b = new BigDecimal("-1060644711");
        Field field = new Field("test",
                FieldType.nullable(new ArrowType.Decimal(38, 0, 128)),
                ImmutableList.of());
        DecimalVector expectedVector = new DecimalVector(field, this.allocator);

        expectedVector.allocateNew(1);
        expectedVector.setValueCount(1);
        expectedVector.set(0, b);
        Int128ArrayBlock dictionary = new Int128ArrayBlock(2, Optional.empty(),
                new long[] {-1L, b.unscaledValue().longValue(), 0L, 0L});
        Block block = DictionaryBlock.create(2, dictionary, new int[] {0, 1});
        this.converter.convert(type, block, 2, vector, Optional.empty());
        assertThat(vector.getValueCount()).isEqualTo(2);
        LongBuffer buffer = vector.getDataBuffer().nioBuffer().order(
                ByteOrder.LITTLE_ENDIAN).asLongBuffer();
        assertThat(buffer.get(0)).isEqualTo(b.unscaledValue().longValue());
        assertThat(buffer.get(1)).isEqualTo(-1L);
        LongBuffer expectedBuffer = expectedVector
                .getDataBuffer()
                .nioBuffer()
                .order(ByteOrder.LITTLE_ENDIAN)
                .asLongBuffer();
        assertThat(buffer.get(0)).isEqualTo(expectedBuffer.get(0));
        assertThat(buffer.get(1)).isEqualTo(expectedBuffer.get(1));
        expectedVector.close();
    }

    @Test
    public void testDictionaryBlockWithNullWithParent()
    {
        Int128ArrayBlock dictionary = new Int128ArrayBlock(4,
                Optional.of(new boolean[] {false, false, true, false}),
                new long[] {0L, 1L, 0L, 2L, 0L, 0L, 0L, 4L});
        Block block = DictionaryBlock.create(4, dictionary,
                new int[] {0, 1, 2, 3});
        Block parentBlock = RowBlock.fromNotNullSuppressedFieldBlocks(4,
                Optional.of(new boolean[] {false, false, false, false}),
                new Block[] {block});
        this.converter.convert(type, block, 4, vector,
                Optional.of(parentBlock));
        assertThat(vector.getValueCount()).isEqualTo(4);
        LongBuffer buffer = vector.getDataBuffer().nioBuffer().order(
                ByteOrder.LITTLE_ENDIAN).asLongBuffer();
        assertThat(buffer.get(0)).isEqualTo(1L);
        assertThat(buffer.get(1)).isEqualTo(0L);

        assertThat(vector.isNull(1)).isFalse();
        assertThat(buffer.get(2)).isEqualTo(2L);
        assertThat(buffer.get(3)).isEqualTo(0L);
        assertThat(vector.isNull(3)).isFalse();
        assertThat(buffer.get(6)).isEqualTo(4L);
        assertThat(buffer.get(7)).isEqualTo(0L);
    }

    @Test
    public void testRLEBlockWithParent()
    {
        RunLengthEncodedBlock block = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(
                DECIMAL_TYPE,
                Decimals.encodeScaledValue(new BigDecimal("1.0"), 1), 4);
        RunLengthEncodedBlock parentBlock = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(
                DECIMAL_TYPE, null, 4);
        this.converter.convert(type, block, 4, vector,
                Optional.of(parentBlock));
        assertThat(vector.getValueCount()).isEqualTo(4);
        assertThat(vector.isNull(0)).isTrue();
        assertThat(vector.isNull(3)).isTrue();
    }

    @Test
    public void testAllNulls()
    {
        Block block = new Int128ArrayBlock(4,
                Optional.of(new boolean[] {true, true, true, true}),
                new long[] {0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L});
        this.converter.convert(type, block, 4, vector, Optional.empty());
        assertThat(vector.getValueCount()).isEqualTo(4);
        assertThat(vector.isNull(0)).isTrue();
        assertThat(vector.isNull(3)).isTrue();
    }

    @Test
    public void testShortDecimalWithOverAllocatedBackingArray()
    {
        // Simulate a LongArrayBlock whose internal values array is larger than positionCount.
        // This can happen when Trino over-allocates the backing array (e.g., during LIMIT processing).
        // Before the fix, this caused a BufferOverflowException in copyShortWithoutParent.
        DecimalType shortDecimalType = DecimalType.createDecimalType(10, 2);
        Field shortField = new Field("test_short", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), ImmutableList.of());
        try (DecimalVector shortVector = new DecimalVector(shortField, this.allocator)) {
            int positionCount = 2;
            // Backing array has 5 elements, but only 2 positions are used
            long[] overAllocatedValues = new long[] {100L, 200L, 300L, 400L, 500L};
            Block block = new LongArrayBlock(positionCount, Optional.empty(), overAllocatedValues);

            // This should NOT throw BufferOverflowException
            converter.convert(shortDecimalType, block, positionCount, shortVector, Optional.empty());

            assertThat(shortVector.getValueCount()).isEqualTo(positionCount);
            LongBuffer buffer = shortVector.getDataBuffer().nioBuffer().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
            assertThat(buffer.get(0)).isEqualTo(100L);
            assertThat(buffer.get(1)).isEqualTo(0L); // sign extension for positive value
            assertThat(buffer.get(2)).isEqualTo(200L);
            assertThat(buffer.get(3)).isEqualTo(0L);
        }
    }
}
