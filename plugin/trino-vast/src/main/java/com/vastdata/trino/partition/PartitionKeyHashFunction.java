/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.partition;

import com.vastdata.trino.partition.PartitionTransforms.ValueTransform;
import io.trino.spi.Page;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.TypeOperators;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.function.BiFunction;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static java.util.Objects.requireNonNull;

public final class PartitionKeyHashFunction
        implements BiFunction<Page, Integer, Long>
{
    private final List<PartitionColumnHashFunction> functions;

    public PartitionKeyHashFunction(List<PartitionColumnHashFunction> functions)
    {
        if (requireNonNull(functions, "functions is null").isEmpty()) {
            throw new IllegalArgumentException("functions is empty");
        }
        this.functions = List.copyOf(functions);
    }

    public static PartitionKeyHashFunction create(List<VastPartitionFunction> partitionFunctions,
                                                  TypeOperators typeOperators,
                                                  IndexBase indexBase)
    {
        List<PartitionColumnHashFunction> partitionColumnHashFunctions = partitionFunctions
                .stream()
                .map(f -> PartitionColumnHashFunction.create(f, indexBase,
                        typeOperators))
                .collect(toImmutableList());

        return new PartitionKeyHashFunction(partitionColumnHashFunctions);
    }

    @Override
    public Long apply(Page page, Integer position)
    {
        return functions
                .stream()
                .mapToLong(f -> f.apply(page, position))
                .reduce(0L, (h, v) -> 31 * h + v);
    }

    public enum IndexBase
    {
        BY_COLUMN_INDEX, BY_PARTITION_INDEX
    }

    private static class PartitionColumnHashFunction
            implements BiFunction<Page, Integer, Long>

    {
        private final Integer blockIdx;
        private final ValueTransform valueTransform;
        private final MethodHandle hashCodeOperator;

        public PartitionColumnHashFunction(Integer blockIdx,
                                           ValueTransform valueTransform,
                                           MethodHandle hashCodeOperator)
        {
            this.blockIdx = requireNonNull(blockIdx, "blockIdx is null");
            this.valueTransform = requireNonNull(valueTransform,
                    "valueTransform is null");
            this.hashCodeOperator = requireNonNull(hashCodeOperator,
                    "hashCodeOperator is null");
        }

        public static PartitionColumnHashFunction create(VastPartitionFunction partitionFunction,
                                                         IndexBase indexBase,
                                                         TypeOperators typeOperators)
        {
            PartitionTransforms.ColumnTransform columnTransform = PartitionTransforms.getColumnTransform(
                    partitionFunction);

            Integer blockIdx = switch (indexBase) {
                case BY_COLUMN_INDEX -> partitionFunction.columnIdx();
                case BY_PARTITION_INDEX -> partitionFunction.partitionIdx();
            };

            MethodHandle hashFunc = typeOperators.getHashCodeOperator(
                    columnTransform.type(),
                    simpleConvention(FAIL_ON_NULL, NEVER_NULL));

            return new PartitionColumnHashFunction(blockIdx,
                    columnTransform.valueTransform(), hashFunc);
        }

        private Object getTransformedValue(Page page, int position)
        {
            Block block = page.getBlock(blockIdx);
            return valueTransform.apply(block, position);
        }

        @Override
        public Long apply(Page page, Integer position)
        {
            return computeHash(getTransformedValue(page, position));
        }

        private Long computeHash(Object value)
        {
            if (value == null) {
                return (long) NULL_HASH_CODE;
            }
            try {
                return (long) hashCodeOperator.invoke(value);
            }
            catch (Throwable throwable) {
                if (throwable instanceof Error error) {
                    throw error;
                }
                throw new TrinoException(
                        StandardErrorCode.GENERIC_INTERNAL_ERROR,
                        "Failed to compute hash", throwable);
            }
        }
    }
}
