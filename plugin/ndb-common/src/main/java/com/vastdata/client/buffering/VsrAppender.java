/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.buffering;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Closeable;

public interface VsrAppender
        extends Closeable
{
    Integer getRowCount();

    void append(VectorSchemaRoot root);

    Schema getSchema();

    void close();
}
