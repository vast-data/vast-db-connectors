/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.block.arrowconverter;

import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.commons.lang3.function.TriConsumer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTimeArrowToBlockConverter
{
    private final TimeArrowToBlockConverter converter = new TimeArrowToBlockConverter();
    private RootAllocator allocator;

    @DataProvider
    public static Object[][] types()
    {
        TriConsumer<BaseFixedWidthVector, Integer, Long> timeSecondSetter = (vector, position, value) -> ((TimeSecVector) vector).set(
                position, value.intValue());
        TriConsumer<BaseFixedWidthVector, Integer, Long> timeMilliSetter = (vector, position, value) -> ((TimeMilliVector) vector).set(
                position, value.intValue());
        TriConsumer<BaseFixedWidthVector, Integer, Long> timeMicroSetter = (vector, position, value) -> ((TimeMicroVector) vector).set(
                position, value.intValue());
        TriConsumer<BaseFixedWidthVector, Integer, Long> timeNanoSetter = (vector, position, value) -> ((TimeNanoVector) vector).set(
                position, value.intValue());
        return new Object[][] {new Object[] {TimeType.TIME_SECONDS,
                new TimeSecVector("seconds", new RootAllocator()),
                timeSecondSetter,
                1_000_000_000_000L},
                new Object[] {TimeType.TIME_MILLIS, new TimeMilliVector("milli",
                        new RootAllocator()), timeMilliSetter, 1_000_000_000L},
                new Object[] {TimeType.TIME_MICROS, new TimeMicroVector("micro",
                        new RootAllocator()), timeMicroSetter, 1_000_000L},
                new Object[] {TimeType.TIME_NANOS, new TimeNanoVector("nano",
                        new RootAllocator()), timeNanoSetter, 1_000L}};
    }

    @BeforeMethod
    public void setup()
    {
        this.allocator = new RootAllocator();
    }

    @Test(dataProvider = "types")
    public void testTypes(Type timeType, BaseFixedWidthVector fieldVector,
            TriConsumer<BaseFixedWidthVector, Integer, Long> valueSetter,
            long factor)
    {
        fieldVector.allocateNew(3);
        valueSetter.accept(fieldVector, 0, 1L);
        valueSetter.accept(fieldVector, 1, 2L);
        valueSetter.accept(fieldVector, 2, 3L);
        fieldVector.setValueCount(3);
        LongArrayBlock block = (LongArrayBlock) converter.convert(timeType,
                List.of(fieldVector), 3, Optional.empty());
        assertThat(block.getLong(0)).isEqualTo(factor);
        assertThat(block.getLong(1)).isEqualTo(2 * factor);
        assertThat(block.getLong(2)).isEqualTo(3 * factor);
    }

    @Test
    public void testMilliVectorWithNulls()
    {
        TimeMilliVector milliVector = new TimeMilliVector("milli", allocator);
        milliVector.allocateNew(3);
        milliVector.set(0, 1);
        milliVector.setNull(1);
        milliVector.set(2, 3);
        milliVector.setValueCount(3);
        LongArrayBlock block = (LongArrayBlock) converter.convert(
                TimeType.TIME_MILLIS, List.of(milliVector), 3,
                Optional.empty());
        assertThat(block.getLong(0)).isEqualTo(1_000_000_000L);
        assertThat(block.isNull(1)).isTrue();
        assertThat(block.getLong(2)).isEqualTo(3_000_000_000L);
    }

    @Test
    public void testMilliVectorWithParentNulls()
    {
        TimeMilliVector milliVector = new TimeMilliVector("milli", allocator);
        milliVector.allocateNew(3);
        milliVector.set(0, 1);
        milliVector.set(1, 2);
        milliVector.setNull(2);
        milliVector.setValueCount(3);
        boolean[] parentNulls = new boolean[] {false, true, false};
        LongArrayBlock block = (LongArrayBlock) converter.convert(
                TimeType.TIME_MILLIS, List.of(milliVector), 3,
                Optional.of(parentNulls));
        assertThat(block.getLong(0)).isEqualTo(1_000_000_000L);
        assertThat(block.isNull(1)).isTrue();
        assertThat(block.isNull(2)).isTrue();
    }

    @Test
    public void testMultiMilliVectorWithNullsAndParentNulls()
    {
        TimeMilliVector firstVector = new TimeMilliVector("milli", allocator);
        firstVector.allocateNew(2);
        firstVector.set(0, 1);
        firstVector.setNull(1);
        firstVector.setValueCount(2);
        TimeMilliVector secondVector = new TimeMilliVector("milli", allocator);
        secondVector.allocateNew(1);
        secondVector.setNull(0);
        secondVector.setValueCount(1);
        boolean[] parentNulls = new boolean[] {false, true, false};
        LongArrayBlock block = (LongArrayBlock) converter.convert(
                TimeType.TIME_MILLIS, List.of(firstVector, secondVector), 3,
                Optional.of(parentNulls));
        assertThat(block.getLong(0)).isEqualTo(1_000_000_000L);
        assertThat(block.isNull(1)).isTrue();
        assertThat(block.isNull(2)).isTrue();
    }

    @Test
    public void testMultiMilliVector()
    {
        TimeMilliVector firstVector = new TimeMilliVector("milli", allocator);
        firstVector.allocateNew(3);
        firstVector.set(0, 0);
        firstVector.set(1, 1);
        firstVector.set(2, 2);
        firstVector.setValueCount(3);
        TimeMilliVector secondVector = new TimeMilliVector("milli", allocator);
        secondVector.allocateNew(3);
        secondVector.set(0, 3);
        secondVector.set(1, 4);
        secondVector.set(2, 5);
        secondVector.setValueCount(3);
        LongArrayBlock block = (LongArrayBlock) converter.convert(
                TimeType.TIME_MILLIS, List.of(firstVector, secondVector), 6,
                Optional.empty());
        assertThat(block.getLong(0)).isEqualTo(0L);
        assertThat(block.getLong(3)).isEqualTo(3_000_000_000L);
    }
}
