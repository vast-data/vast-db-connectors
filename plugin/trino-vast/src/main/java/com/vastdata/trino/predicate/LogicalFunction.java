/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.vastdata.trino.VastColumnHandle;

import java.util.List;
import java.util.Objects;

public class LogicalFunction
        implements ComplexPredicate
{
    String name;
    List<ComplexPredicate> children;

    @JsonCreator
    public LogicalFunction(
            @JsonProperty("name") String name,
            @JsonProperty("children") List<ComplexPredicate> children)
    {
        this.name = name;
        this.children = children;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<ComplexPredicate> getChildren()
    {
        return children;
    }

    @Override
    public void collectColumns(ImmutableSet.Builder<VastColumnHandle> result)
    {
        children.forEach(child -> child.collectColumns(result));
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
        LogicalFunction that = (LogicalFunction) o;
        return Objects.equals(name, that.name) && Objects.equals(children, that.children);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, children);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("children", children)
                .toString();
    }
}
