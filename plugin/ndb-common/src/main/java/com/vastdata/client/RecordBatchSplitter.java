/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.google.common.collect.ImmutableList;
import com.vastdata.client.error.VastTooLargePageException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.metrics.RecordBatchSplitterMetrics;
import com.vastdata.client.schema.VastPayloadSerializer;
import io.airlift.log.Logger;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Optional;

import static java.lang.String.format;

public class RecordBatchSplitter
{
    private static final Logger LOG = Logger.get(RecordBatchSplitter.class);

    private final long maxSerializedSize;
    private final VastPayloadSerializer<VectorSchemaRoot> serializer;
    private final Optional<RecordBatchSplitterMetrics> metrics;

    public RecordBatchSplitter(long maxSerializedSize,
            VastPayloadSerializer<VectorSchemaRoot> serializer)
    {
        this(maxSerializedSize, serializer, Optional.empty());
    }

    public RecordBatchSplitter(long maxSerializedSize,
            VastPayloadSerializer<VectorSchemaRoot> serializer,
            Optional<RecordBatchSplitterMetrics> metrics)
    {
        this.maxSerializedSize = maxSerializedSize;
        this.serializer = serializer;
        this.metrics = metrics;
    }

    public List<byte[]> split(VectorSchemaRoot root, int maxRowCount)
            throws VastUserException
    {
        Schema originalSchema = root.getSchema();
        ImmutableList.Builder<byte[]> builder = ImmutableList.builder();
        int offset = 0;
        int size = Math.min(maxRowCount, root.getRowCount());
        while (offset < root.getRowCount()) {
            // try to serialize a non-empty RecordBatch
            size = Math.min(size, root.getRowCount() - offset);
            while (size > 0) {
                VectorSchemaRoot slice = root.slice(offset, size);
                // ORION-114731: Arrow Java implementation replaces internal List field name to `$data$`, instead of the standard `item` name.
                VectorSchemaRoot sliceWithOriginalSchema = new VectorSchemaRoot(
                        originalSchema, slice.getFieldVectors(),
                        slice.getRowCount());
                try {
                    byte[] serialized = serializer
                            .apply(sliceWithOriginalSchema)
                            .get();
                    if (serialized.length <= maxSerializedSize) {
                        metrics.ifPresent(m -> m.recordSerializedBatch(
                                serialized.length));
                        builder.add(serialized);
                        offset += size;
                        break;
                    }
                    metrics.ifPresent(m -> m.recordOversizedBatchAttempt(
                            serialized.length));
                    LOG.warn(
                            "Too large RecordBatch chunk [%d,%d): %d bytes - retrying with less rows",
                            offset, offset + size, serialized.length);
                    size /= 2; // reduce # of rows, and retry
                }
                finally {
                    if (slice != root) {
                        slice.close(); // root.slice(0, root.getRowCount()) returns root, so we can't close it.
                    }
                }
            }
            if (size == 0) {
                throw new VastTooLargePageException(
                        format("Failed to serialize too large RecordBatch: %s rows=%d",
                                root.getSchema(), root.getRowCount()));
            }
        }
        return builder.build();
    }
}
