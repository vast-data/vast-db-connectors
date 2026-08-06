/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.vastdata.trino.VastColumnHandle;
import io.trino.spi.predicate.Domain;

import static com.google.common.base.MoreObjects.toStringHelper;

public record ColumnDomain(VastColumnHandle column, Domain domain)
        implements ComplexPredicate
{
    @JsonCreator
    public ColumnDomain(@JsonProperty("column") VastColumnHandle column,
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
    public String toString()
    {
        return toStringHelper(this)
                .add("column", column.getField())
                .add("domain", domain)
                .toString();
    }
}
