/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.adaptor;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.Optional;

@FunctionalInterface
public interface VectorAdaptorFactory
{
    Optional<VectorAdaptor> forField(Field field);
}
