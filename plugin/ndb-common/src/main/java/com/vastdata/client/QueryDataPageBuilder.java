/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import org.apache.arrow.vector.VectorSchemaRoot;

public interface QueryDataPageBuilder<T>
{
    QueryDataPageBuilder<T> add(VectorSchemaRoot root);
    T build();
    void clear();
}
