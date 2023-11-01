/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.predicate;

import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.StructField;

import java.io.Serializable;

public class VastPredicate
        implements Serializable
{
    private final Predicate predicate;
    private final NamedReference reference;
    private final StructField field;

    public VastPredicate(Predicate predicate, NamedReference reference, StructField field)
    {
        this.predicate = predicate;
        this.reference = reference;
        this.field = field;
    }

    public Predicate getPredicate()
    {
        return predicate;
    }

    public NamedReference getReference()
    {
        return reference;
    }

    public StructField getField()
    {
        return field;
    }

    public String toString() {
        return this.getPredicate().toString();
    }
}
