/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.vastdata.client.VerifyParam;
import com.vastdata.client.error.VastUserException;
import io.airlift.log.Logger;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types;

import java.util.Set;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static java.lang.String.format;

public final class RowIDVectorCopy
{
    private static final Logger LOG = Logger.get(RowIDVectorCopy.class);
    private static final Set<Types.MinorType> allowedTypes = ImmutableSet.of(Types.MinorType.BIGINT, Types.MinorType.UINT8);

    private RowIDVectorCopy() {}

    private static boolean verifyFieldType(FieldVector vector)
    {
        return allowedTypes.contains(vector.getMinorType());
    }

    public static FieldVector copyVectorBuffers(FieldVector fieldVector, FieldVector newVector)
    {
        try {
            VerifyParam.verify(verifyFieldType(fieldVector), format("Copy source vector type is not 64bit integer: %s", fieldVector.getMinorType()));
            VerifyParam.verify(verifyFieldType(newVector), format("Copy destination vector type is not 64bit integer: %s", newVector.getMinorType()));
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }

        int valueCount = fieldVector.getValueCount();
        ArrowBuf curDataBuf = fieldVector.getDataBuffer();
        ArrowBuf curValidityBuf = fieldVector.getValidityBuffer();

        ArrowFieldNode node = new ArrowFieldNode(valueCount, 0); // row_id values are not null

        ImmutableList<ArrowBuf> buffers = ImmutableList.of(curValidityBuf, curDataBuf);
        newVector.loadFieldBuffers(node, buffers);
        newVector.setValueCount(valueCount);
        LOG.debug("%s Polled row id field of buffer length: %s, transformed to spark vector buffer of length: %s, value count: %s, new vector: %s",
                Thread.currentThread().getName(), fieldVector.getBufferSize(), newVector.getBufferSize(), valueCount, newVector);
        return newVector;
    }
}
