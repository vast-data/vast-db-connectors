/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

import com.vastdata.client.QueryDataExtraParams;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastException;
import com.vastdata.client.tx.VastTransaction;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Optional;
import java.util.function.Function;

public class VastDeleteWrite
        implements VastWriteStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger(VastDeleteWrite.class);

    private final WriteContext ctx;

    public VastDeleteWrite(
            Function<VastConfig, VastClient> vastClientSupplier, String dataWriteTraceToken, VastConfig vastConfig, URI endpoint, VastTransaction tx, String schemaName,
            String tableName, String endUser)
    {
        this.ctx = new WriteContext(vastClientSupplier, dataWriteTraceToken, vastConfig, endpoint, tx, schemaName, tableName, endUser);
    }

    @Override
    public void write(VectorSchemaRoot nextChunk)
            throws VastException
    {
        int columnCount = nextChunk.getFieldVectors().size();
        LOG.debug("{} Deleting next chunk of {} rows, {} columns, hash={}, schema: {}",
                ctx.getDataWriteTraceToken(),
                nextChunk.getRowCount(),
                columnCount,
                nextChunk.hashCode(),
                nextChunk.getSchema()
        );
        try (nextChunk) {
            ctx.getVastClientSupplier()
                    .apply(ctx.getVastConfig())
                    .deleteRows(ctx.getTx(), ctx.getSchemaName(), ctx.getTableName(), nextChunk, ctx.getEndpoint(),
                            Optional.empty(), new QueryDataExtraParams(), ctx.getEndUser());
        }
    }
}
