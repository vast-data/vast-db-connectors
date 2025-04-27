/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.vdb.sdk;

import com.vastdata.client.adaptor.VectorAdaptor;
import com.vastdata.client.adaptor.VectorAdaptorFactory;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.Optional;

public final class EmptyVectorAdaptorFactory
        implements VectorAdaptorFactory
{
    public EmptyVectorAdaptorFactory() {}

    public Optional<VectorAdaptor> forField(Field field)
    {
        return Optional.empty();
    }
}
