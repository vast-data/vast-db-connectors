/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino.predicate;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableSet;
import com.vastdata.trino.VastColumnHandle;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ColumnDomain.class, name = "domain"),
        @JsonSubTypes.Type(value = LogicalFunction.class, name = "function")})
public interface ComplexPredicate
{
    void collectColumns(ImmutableSet.Builder<VastColumnHandle> result);
}
