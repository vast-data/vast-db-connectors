/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.metrics.ByColumnInserterMetrics;
import com.vastdata.client.metrics.RecordBatchSplitterMetrics;
import com.vastdata.client.rowid.RowIDStrategyType;
import com.vastdata.client.tx.VastTransaction;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

public final class VastBGWriterFactory
{
    private VastBGWriterFactory()
    {
    }

    public static VastBGWriter forImport(
            int ordinal,
            Function<VastConfig, VastClient> vastClientSupplier,
            String dataWriteTraceToken, VastConfig vastConfig, URI endpoint,
            VastTransaction tx, String schemaName, String tableName,
            Supplier<VectorSchemaRoot> insertArrowVectorsQ
    )
    {
        return new VastBGWriter(
                ordinal, dataWriteTraceToken, insertArrowVectorsQ, new VastImportWrite(
                vastClientSupplier, dataWriteTraceToken, vastConfig, endpoint, tx, schemaName,
                tableName
        )
        );
    }

    public static VastBGWriter forInsert(int ordinal,
                                         Function<VastConfig, VastClient> vastClientSupplier,
                                         String dataWriteTraceToken, VastConfig vastConfig, List<URI> dataEndpoints,
                                         VastTransaction tx, String schemaName, String tableName,
                                         Supplier<VectorSchemaRoot> insertArrowVectorsQ,
                                         String endUser,
                                         Set<String> nonUpdatableColumns,
                                         RowIDStrategyType rowIdType,
                                         RecordBatchSplitterMetrics splitterMetrics,
                                         ByColumnInserterMetrics insertMetrics,
                                         ExecutorService ioExecutor,
                                         ExecutorService cpuExecutor
    )
    {
        return new VastBGWriter(
                ordinal, dataWriteTraceToken, insertArrowVectorsQ, new VastInsertWrite(
                vastClientSupplier, dataWriteTraceToken, vastConfig, dataEndpoints, tx, schemaName,
                tableName, endUser, nonUpdatableColumns, rowIdType,
                splitterMetrics, insertMetrics, ioExecutor, cpuExecutor
        )
        );
    }

    public static VastBGWriter forUpdate(int ordinal,
            Function<VastConfig, VastClient> vastClientSupplier,
            String dataWriteTraceToken, VastConfig vastConfig, URI endpoint,
            VastTransaction tx, String schemaName, String tableName,
            Supplier<VectorSchemaRoot> insertArrowVectorsQ, String endUser)
    {
        return new VastBGWriter(
                ordinal, dataWriteTraceToken, insertArrowVectorsQ, new VastUpdateWrite(
                vastClientSupplier, dataWriteTraceToken, vastConfig, endpoint, tx, schemaName,
                tableName, endUser
        )
        );
    }

    public static VastBGWriter forDelete(int ordinal,
            Function<VastConfig, VastClient> vastClientSupplier,
            String dataWriteTraceToken, VastConfig vastConfig, URI endpoint,
            VastTransaction tx, String schemaName, String tableName,
            Supplier<VectorSchemaRoot> insertArrowVectorsQ, String endUser)
    {
        return new VastBGWriter(
                ordinal, dataWriteTraceToken, insertArrowVectorsQ, new VastDeleteWrite(
                vastClientSupplier, dataWriteTraceToken, vastConfig, endpoint, tx, schemaName,
                tableName, endUser
        )
        );
    }
}
