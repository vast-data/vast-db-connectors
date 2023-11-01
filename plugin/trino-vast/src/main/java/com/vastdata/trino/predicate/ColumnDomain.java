/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.vastdata.trino.VastColumnHandle;
import io.trino.spi.predicate.Domain;

import java.util.Objects;

public class ColumnDomain
        implements ComplexPredicate
{
    private final VastColumnHandle column;
    private final Domain domain;

    @JsonCreator
    public ColumnDomain(
            @JsonProperty("column") VastColumnHandle column,
            @JsonProperty("domain") Domain domain)
    {
        this.column = column;
        this.domain = domain;
    }

    @JsonProperty
    public VastColumnHandle getColumn()
    {
        return column;
    }

    @JsonProperty
    public Domain getDomain()
    {
        return domain;
    }

    @Override
    public void collectColumns(ImmutableSet.Builder<VastColumnHandle> result)
    {
        result.add(column);
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
        ColumnDomain that = (ColumnDomain) o;
        return Objects.equals(column, that.column) && Objects.equals(domain, that.domain);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, domain);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("column", column.getField())
                .add("domain", domain)
                .toString();
    }
}
