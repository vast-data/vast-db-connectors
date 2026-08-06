/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.vastdata.trino.VastColumnHandle;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public record LogicalFunction(String name,
        List<ComplexPredicate> children)
        implements ComplexPredicate
{
    @JsonCreator
    public LogicalFunction(@JsonProperty("name") String name,
                           @JsonProperty("children") List<ComplexPredicate> children)
    {
        this.name = name;
        this.children = children;
    }

    @Override
    @JsonProperty
    public String name()
    {
        return name;
    }

    @Override
    @JsonProperty
    public List<ComplexPredicate> children()
    {
        return children;
    }

    @Override
    public void collectColumns(ImmutableSet.Builder<VastColumnHandle> result)
    {
        children.forEach(child -> child.collectColumns(result));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("children", children)
                .toString();
    }
}
