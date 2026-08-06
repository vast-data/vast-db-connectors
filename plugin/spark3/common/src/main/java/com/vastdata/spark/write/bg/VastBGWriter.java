/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

import com.vastdata.client.error.VastException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static java.lang.String.format;

public class VastBGWriter
        extends ParallelWriteExecutionComponent
{
    private static final Logger LOG = LoggerFactory.getLogger(
            VastBGWriter.class);

    private final Supplier<VectorSchemaRoot> insertChuncksSupplier;
    private final String dataWriteTraceToken;
    private int processedRows = 0;
    private final VastWriteStrategy vastWriteStrategy;

    VastBGWriter(int ordinal,
                 String dataWriteTraceToken,
                 Supplier<VectorSchemaRoot> insertChuncksSupplier,
                 VastWriteStrategy vastWriteStrategy)
    {
        super(ordinal);
        this.dataWriteTraceToken = dataWriteTraceToken;
        this.insertChuncksSupplier = insertChuncksSupplier;
        this.vastWriteStrategy = vastWriteStrategy;
    }
    @Override
    public void doRun()
    {
        VectorSchemaRoot nextChunk = null;
        try {
            for (nextChunk = pollNext(); nextChunk != null; nextChunk = pollNext()) {
                int rowCount = nextChunk.getRowCount();
                try {
                    vastWriteStrategy.write(nextChunk);
                }
                catch (VastException e) {
                    throw toRuntime(e);
                }
                countProcessedRows(rowCount);
            }
        }
        finally {
            // Close a chunk only if it was polled but never handed off to the strategy.
            if (nextChunk != null) {
                nextChunk.close();
            }
            for (nextChunk = pollNext(); nextChunk != null; nextChunk = pollNext()) {
                nextChunk.close();
            }
        }
        LOG.debug(
                "{} was signalled to stop, exiting",
                dataWriteTraceToken
        );
    }

    private void countProcessedRows(int rowCount)
    {
        processedRows += rowCount;
        LOG.debug(
                "{} Processed chunk rows: {}, total processed rows: {}",
                dataWriteTraceToken, rowCount,
                processedRows
        );
    }

    @Override
    public String getTaskName()
    {
        return format("%s", dataWriteTraceToken);
    }

    private VectorSchemaRoot pollNext()
    {
        VectorSchemaRoot vectorSchemaRoot = this.insertChuncksSupplier.get();
        if (vectorSchemaRoot != null) {
            LOG.debug(
                    "{} polled new chunk of {} rows, hash={}"
                    , dataWriteTraceToken,
                    vectorSchemaRoot.getRowCount(),
                    vectorSchemaRoot.hashCode());
        }
        else {
            LOG.debug("{} polled null", dataWriteTraceToken);
        }
        return vectorSchemaRoot;
    }
}
