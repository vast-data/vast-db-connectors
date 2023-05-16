/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class VastSplitContext
{
    private final int currentSplit;
    private final int numOfSplits;
    private final int numOfSubSplits;
    private final int rowGroupsPerSubSplit;

    @JsonCreator
    public VastSplitContext(
            @JsonProperty("currentSplit") int currentSplit,
            @JsonProperty("numOfSplits") int numOfSplits,
            @JsonProperty("numOfSubSplits") int numOfSubSplits,
            @JsonProperty("rowGroupsPerSubSplit") int rowGroupsPerSubSplit)
    {
        this.currentSplit = currentSplit;
        this.numOfSplits = numOfSplits;
        this.numOfSubSplits = numOfSubSplits;
        this.rowGroupsPerSubSplit = rowGroupsPerSubSplit;
    }

    @JsonProperty
    public int getCurrentSplit()
    {
        return currentSplit;
    }

    @JsonProperty
    public int getNumOfSplits()
    {
        return numOfSplits;
    }

    @JsonProperty
    public int getNumOfSubSplits()
    {
        return numOfSubSplits;
    }

    @JsonProperty
    public int getRowGroupsPerSubSplit()
    {
        return rowGroupsPerSubSplit;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VastSplitContext that = (VastSplitContext) o;
        return currentSplit == that.currentSplit && numOfSplits == that.numOfSplits && numOfSubSplits == that.numOfSubSplits && rowGroupsPerSubSplit == that.rowGroupsPerSubSplit;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(currentSplit, numOfSplits, numOfSubSplits, rowGroupsPerSubSplit);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("current", currentSplit)
                .add("total", numOfSplits)
                .add("subSplits", numOfSubSplits)
                .add("rowGroups", rowGroupsPerSubSplit)
                .toString();
    }
}
