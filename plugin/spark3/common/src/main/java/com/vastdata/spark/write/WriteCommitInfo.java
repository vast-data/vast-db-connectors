/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.spark.write;

import java.util.StringJoiner;

public class WriteCommitInfo
{
    private final int writeIndex;
    private final String traceToken;
    private final int writtenRows;

    public WriteCommitInfo(int writeIndex, String traceToken, int writtenRows) {
        this.writeIndex = writeIndex;
        this.traceToken = traceToken;
        this.writtenRows = writtenRows;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", WriteCommitInfo.class.getSimpleName() + "[", "]")
                .add("writeIndex=" + writeIndex)
                .add("traceToken='" + traceToken + "'")
                .add("writtenRows=" + writtenRows)
                .toString();
    }
}
