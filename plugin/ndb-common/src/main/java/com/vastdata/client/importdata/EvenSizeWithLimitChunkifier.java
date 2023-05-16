/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.importdata;

import com.google.common.collect.Lists;
import com.vastdata.client.schema.ImportDataContext;
import com.vastdata.client.schema.ImportDataFile;
import io.airlift.log.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class EvenSizeWithLimitChunkifier
        implements BiFunction<ImportDataContext, List<URI>, List<ImportDataContext>>
{
    private static final Logger LOG = Logger.get(EvenSizeWithLimitChunkifier.class);

    // Previous to https://vastdata.atlassian.net/browse/ORION-106056 the value was 42 - Answer to the Ultimate Question of Life, the Universe, and Everything, and also 14 silos * 3 = max parallelism per request
    // Degradation since https://vastdata.atlassian.net/browse/ORION-104707 - in which S3_TABULAR_GATE_KEEPER_IMPORT_SUB_REQ_ALLOWED_COUNT was reduced to 10
    public static final int CHUNK_SIZE_LIMIT = 10;

    @Override
    public List<ImportDataContext> apply(ImportDataContext importDataContext, List<URI> endpoints)
    {
        Collection<List<ImportDataFile>> partition = getPartitions(importDataContext, endpoints);
        return partition.stream()
                .map(filesChunk -> new ImportDataContext(filesChunk, importDataContext.getDest()))
                .collect(Collectors.toList());
    }

    private Collection<List<ImportDataFile>> getPartitions(ImportDataContext importDataContext, List<URI> endpoints)
    {
        int limit = importDataContext.getChunkLimit().orElse(CHUNK_SIZE_LIMIT);
        Collection<List<ImportDataFile>> partition;
        List<ImportDataFile> sourceFiles = importDataContext.getSourceFiles();
        if (sourceFiles.size() <= endpoints.size() * limit) {
            Map<Integer, List<ImportDataFile>> chunksCollector = new HashMap<>(endpoints.size());
            for (int i = 0; i < sourceFiles.size(); i++) {
                ImportDataFile file = sourceFiles.get(i);
                chunksCollector.computeIfAbsent(i % endpoints.size(), ArrayList::new).add(file);
            }
            partition = chunksCollector.values();
        }
        else {
            partition = Lists.partition(sourceFiles, limit);
        }
        LOG.debug("split %d files into %d chunks (using limit=%d)", sourceFiles.size(), partition.size(), limit);
        return partition;
    }
}
