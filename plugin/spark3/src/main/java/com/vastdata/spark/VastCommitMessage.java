/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

import static java.lang.String.format;

public class VastCommitMessage
        implements WriterCommitMessage
{
    private final String toString;
    public VastCommitMessage(int splitNo) {
        this.toString = format("VastCommitMessage[%s]", splitNo);
    }

    @Override
    public String toString()
    {
        return toString;
    }
}
