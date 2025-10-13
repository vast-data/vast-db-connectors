/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.tx.VastTransaction;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.net.URI;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vastdata.spark.write.bg.VastWriteMode.DELETE;
import static com.vastdata.spark.write.bg.VastWriteMode.IMPORT;
import static com.vastdata.spark.write.bg.VastWriteMode.INSERT;
import static com.vastdata.spark.write.bg.VastWriteMode.UPDATE;

public final class VastBGWriterFactory
{
    private VastBGWriterFactory() {}

    public static VastBGWriter forInsert(
            int ordinal,
            Function<VastConfig, VastClient> vastClientSupplier,
            String dataWriteTraceToken,
            VastConfig vastConfig,
            URI endpoint,
            VastTransaction tx,
            String schemaName,
            String tableName,
            Supplier<VectorSchemaRoot> insertArrowVectorsQ,
            boolean forImportData)
    {
        if (forImportData) {
            return new VastBGWriter(ordinal, vastClientSupplier, dataWriteTraceToken, vastConfig, endpoint, tx,
                    schemaName, tableName, insertArrowVectorsQ, IMPORT);
        }
        else {
            return new VastBGWriter(ordinal, vastClientSupplier, dataWriteTraceToken, vastConfig, endpoint, tx,
                    schemaName, tableName, insertArrowVectorsQ, INSERT);
        }
    }

    public static VastBGWriter forUpdate(
            int ordinal,
            Function<VastConfig, VastClient> vastClientSupplier,
            String dataWriteTraceToken,
            VastConfig vastConfig,
            URI endpoint,
            VastTransaction tx,
            String schemaName,
            String tableName,
            Supplier<VectorSchemaRoot> insertArrowVectorsQ)
    {
        return new VastBGWriter(ordinal, vastClientSupplier, dataWriteTraceToken, vastConfig, endpoint, tx,
                schemaName, tableName, insertArrowVectorsQ, UPDATE);
    }

    public static VastBGWriter forDelete(
            int ordinal,
            Function<VastConfig, VastClient> vastClientSupplier,
            String dataWriteTraceToken,
            VastConfig vastConfig,
            URI endpoint,
            VastTransaction tx,
            String schemaName,
            String tableName,
            Supplier<VectorSchemaRoot> insertArrowVectorsQ)
    {
        return new VastBGWriter(ordinal, vastClientSupplier, dataWriteTraceToken, vastConfig, endpoint, tx,
                schemaName, tableName, insertArrowVectorsQ, DELETE);
    }
}
