/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

import static java.lang.String.format;

public class VastCommitMessage
        implements WriterCommitMessage
{
    private final String toString;
    public VastCommitMessage(String info) {
        this.toString = format("VastCommitMessage[%s]", info);
    }

    @Override
    public String toString()
    {
        return toString;
    }
}
