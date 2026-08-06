/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class VastObjectDetails
{
    private final String name;
    private final String properties;
    private final String handle;
    private final long numRows;
    private final long sizeInBytes;
    private final long numPartitions;
    private final boolean sortingKeyEnabled;
    private final long sortingScore;
    private final long writeAmplification;
    private final long acummulativeRowInseritionCount;
    private final long mstPointer;

    @JsonCreator
    public VastObjectDetails(
            @JsonProperty("name") String name,
            @JsonProperty("properties") String properties,
            @JsonProperty("handle") String handle,
            @JsonProperty("numRows") long numRows,
            @JsonProperty("sizeInBytes") long sizeInBytes,
            @JsonProperty("numPartitions") long numPartitions,
            @JsonProperty("sortingKeyEnabled") boolean sortingKeyEnabled,
            @JsonProperty("sortingScore") long sortingScore,
            @JsonProperty("writeAmplification") long writeAmplification,
            @JsonProperty("acummulativeRowInseritionCount") long acummulativeRowInseritionCount,
            @JsonProperty("mstPointer") long mstPointer)
    {
        this.name = name;
        this.properties = properties;
        this.handle = handle;
        this.numRows = numRows;
        this.sizeInBytes = sizeInBytes;
        this.numPartitions = numPartitions;
        this.sortingKeyEnabled = sortingKeyEnabled;
        this.sortingScore = sortingScore;
        this.writeAmplification = writeAmplification;
        this.acummulativeRowInseritionCount = acummulativeRowInseritionCount;
        this.mstPointer = mstPointer;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getProperties()
    {
        return properties;
    }

    @JsonProperty
    public String getHandle()
    {
        return handle;
    }

    @JsonProperty
    public long getNumRows()
    {
        return numRows;
    }

    @JsonProperty
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @JsonProperty
    public long getNumPartitions()
    {
        return numPartitions;
    }

    @JsonProperty
    public boolean isSortingKeyEnabled()
    {
        return sortingKeyEnabled;
    }

    @JsonProperty
    public long getSortingScore()
    {
        return sortingScore;
    }

    @JsonProperty
    public long getWriteAmplification()
    {
        return writeAmplification;
    }

    @JsonProperty
    public long getAcummulativeRowInseritionCount()
    {
        return acummulativeRowInseritionCount;
    }

    @JsonProperty
    public long getMstPointer()
    {
        return mstPointer;
    }

    public static VastObjectDetails fromObjectDetails(vast_flatbuf.tabular.ObjectDetails objectDetails)
    {
        return new VastObjectDetails(
                objectDetails.name(),
                objectDetails.properties(),
                objectDetails.handle(),
                objectDetails.numRows(),
                objectDetails.sizeInBytes(),
                objectDetails.numPartitions(),
                objectDetails.sortingKeyEnabled(),
                objectDetails.sortingScore(),
                objectDetails.writeAmplification(),
                objectDetails.acummulativeRowInseritionCount(),
                objectDetails.mstPointer());
    }

    @Override
    public String toString()
    {
        return "VastObjectDetails{" +
                "name='" + name + '\'' +
                ", handle='" + handle + '\'' +
                ", numRows=" + numRows +
                ", sizeInBytes=" + sizeInBytes +
                ", numPartitions=" + numPartitions +
                ", sortingKeyEnabled=" + sortingKeyEnabled +
                ", sortingScore=" + sortingScore +
                ", writeAmplification=" + writeAmplification +
                ", acummulativeRowInseritionCount=" + acummulativeRowInseritionCount +
                ", mstPointer=" + mstPointer +
                '}';
    }
}
