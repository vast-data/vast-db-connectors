/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import java.util.List;
import java.util.Optional;

public class ImportDataContext
{
    private final String dest;
    private final List<ImportDataFile> sourceFiles;
    private final Optional<Integer> chunkLimit;

    private ImportDataContext(List<ImportDataFile> sourceFiles, String dest, Optional<Integer> chunkLimit)
    {
        this.sourceFiles = sourceFiles;
        this.dest = dest;
        this.chunkLimit = chunkLimit;
    }

    public ImportDataContext(List<ImportDataFile> sourceFiles, String dest)
    {
        this(sourceFiles, dest, Optional.empty());
    }

    public ImportDataContext withChunkLimit(int chunkLimit)
    {
        return new ImportDataContext(this.sourceFiles, this.dest, Optional.of(chunkLimit));
    }

    public String getDest()
    {
        return dest;
    }

    public List<ImportDataFile> getSourceFiles()
    {
        return sourceFiles;
    }

    public Optional<Integer> getChunkLimit()
    {
        return chunkLimit;
    }

    @Override
    public String toString()
    {
        return String.format("ImportDataContext(files:%s, dest:%s, chunk:%s)", sourceFiles == null ? 0 : sourceFiles.size(), dest, chunkLimit);
    }
}
