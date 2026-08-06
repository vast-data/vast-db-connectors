/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write.bg;

import com.vastdata.client.error.VastException;
import org.apache.arrow.vector.VectorSchemaRoot;

public interface VastWriteStrategy
{
    void write(VectorSchemaRoot nextChunk)
            throws VastException;
}
