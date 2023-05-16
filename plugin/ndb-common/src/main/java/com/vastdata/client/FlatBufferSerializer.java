/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.google.flatbuffers.FlatBufferBuilder;

public interface FlatBufferSerializer
{
    /**
     * Must be called once per serializer instance.
     */
    int serialize(FlatBufferBuilder builder);
}
