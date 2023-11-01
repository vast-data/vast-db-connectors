/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.importdata;

import com.vastdata.client.schema.ImportDataFile;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.function.IntFunction;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.error.VastExceptionFactory.userException;

public class ImportDataFileMapper implements IntFunction<ImportDataFile>
{
    private final Function<Integer, VectorSchemaRoot> rowSupplier;
    private final int hiddenColumnIndex;

    public ImportDataFileMapper(Function<Integer, VectorSchemaRoot> rowSupplier, int hiddenColumnIndex) {
        this.rowSupplier = rowSupplier;
        this.hiddenColumnIndex = hiddenColumnIndex;
    }

    @Override
    public ImportDataFile apply(int i)
    {
        VectorSchemaRoot vectorSchemaRoot = rowSupplier.apply(i);
        // last field is the column name
        VarCharVector parquetPathVector = (VarCharVector) vectorSchemaRoot.getVector(hiddenColumnIndex);
        String fileName = new String(parquetPathVector.get(0), StandardCharsets.UTF_8);
        String[] split = (fileName.startsWith("/") ? fileName.substring(1) : fileName).split("/", 2);
        if (split.length != 2) {
            vectorSchemaRoot.close();
            throw toRuntime(userException("Invalid source file name string format - bucket is not specified"));
        }
        VectorSchemaRoot partitionsOnlyVectorSchemaRoot = vectorSchemaRoot.removeVector(hiddenColumnIndex);
        return new ImportDataFile(split[0], split[1], partitionsOnlyVectorSchemaRoot);
    }
}
