/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.testng.Assert.assertTrue;

public class TestVastPageBuilder
{
    @DataProvider
    public Object[][] buildIsNullProvider()
    {
        return new Object[][] {
                {0, 1},
                {1, 1},
                {4, 1},
                {8, 1},
                {11, 1},
                {16, 1},
                {17, 1},
                {127, 1},
                {128, 1},
                {129, 1}
        };
    }

    @Test(dataProvider = "buildIsNullProvider")
    public void testBuildIsNull(int vectorSize, int iterations)
    {
        try (
                BufferAllocator allocator = new RootAllocator();
                IntVector intVector = new IntVector("i", allocator)) {
            boolean[] isNull = new boolean[vectorSize];
            Random random = new Random(0);
            intVector.allocateNew(vectorSize);
            for (int i = 0; i < vectorSize; ++i) {
                isNull[i] = random.nextBoolean();
                if (isNull[i]) {
                    intVector.setNull(i);
                }
                else {
                    intVector.set(i, i);
                }
            }
            intVector.setValueCount(vectorSize);
            List<FieldVector> vectors = List.of(intVector);
            int positions = vectorSize * vectors.size();
            Optional<boolean[]> result = Optional.empty();
            long start = System.nanoTime();
            for (int i = 0; i < iterations; ++i) {
                result = VastPageBuilder.buildIsNull(positions, vectors);
            }
            long took = (System.nanoTime() - start);
            boolean[] actual = result.orElse(new boolean[vectorSize]); //empty = no NULLs
            assertTrue(Arrays.equals(actual, isNull));
            System.out.printf("buildIsNull took: %.3f ns / position%n", took / ((double) positions * iterations));
        }
    }
}
