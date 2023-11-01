/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.stats;

public class VastStatistics
{
    private final Long num_rows;

    private final Long size_in_bytes;

    public VastStatistics(Long num_rows, Long size_in_bytes)
    {
        this.num_rows = num_rows;
        this.size_in_bytes = size_in_bytes;
    }

    public Long getNumRows()
    {
        return this.num_rows;
    }

    public Long getSizeInBytes()
    {
        return this.size_in_bytes;
    }

    @Override
    public String toString() {
        return String.format("num_rows={}, size_in_bytes={}", this.getNumRows(), this.getSizeInBytes());
    }
}
