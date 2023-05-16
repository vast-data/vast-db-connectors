/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.importdata;

import com.google.common.collect.ImmutableList;
import com.vastdata.client.schema.ImportDataContext;

import java.net.URI;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class ImportDataFileChunkifierFactory
{
    private ImportDataFileChunkifierFactory() {}

    private static final Map<ImportDataFileChunkifierStrategy, BiFunction<ImportDataContext, List<URI>, List<ImportDataContext>>> strategyImplMap =
            new EnumMap<>(ImportDataFileChunkifierStrategy.class);

    private static final BiFunction<ImportDataContext, List<URI>, List<ImportDataContext>> SINGLE_FILE_PER_REQUEST_IMPL =
            (ctx, endpoints) -> ctx.getSourceFiles().stream().map(f -> new ImportDataContext(ImmutableList.of(f), ctx.getDest())).collect(Collectors.toList());

    private static final BiFunction<ImportDataContext, List<URI>, List<ImportDataContext>> EVEN_CHUNKS_WITH_SIZE_LIMIT_PER_ENDPOINT_IMPL = new EvenSizeWithLimitChunkifier();

    static {
        strategyImplMap.put(ImportDataFileChunkifierStrategy.SINGLE, SINGLE_FILE_PER_REQUEST_IMPL);
        strategyImplMap.put(ImportDataFileChunkifierStrategy.CHUNK, EVEN_CHUNKS_WITH_SIZE_LIMIT_PER_ENDPOINT_IMPL);
    }

    public static BiFunction<ImportDataContext, List<URI>, List<ImportDataContext>> getInstance(ImportDataFileChunkifierStrategy strategy)
    {
        requireNonNull(strategy);
        return strategyImplMap.get(strategy);
    }
}
